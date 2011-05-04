package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;

import lucandra.CassandraUtils;

import org.apache.cassandra.db.*;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.util.FieldCacheSanityChecker;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.core.SolandraCoreContainer;

public class LucandraFieldCache implements FieldCache
{

    private static final Logger  logger = Logger.getLogger(LucandraFieldCache.class);

    private Map<Class<?>, Cache> caches;

    public LucandraFieldCache()
    {
        init();
    }

    private synchronized void init()
    {
        caches = new HashMap<Class<?>, Cache>(7);
        caches.put(Byte.TYPE, new ByteCache(this));
        caches.put(Short.TYPE, new ShortCache(this));
        caches.put(Integer.TYPE, new IntCache(this));
        caches.put(Float.TYPE, new FloatCache(this));
        caches.put(Long.TYPE, new LongCache(this));
        caches.put(Double.TYPE, new DoubleCache(this));
        caches.put(String.class, new StringCache(this));
        caches.put(StringIndex.class, new StringIndexCache(this));
    }

    public synchronized void purgeAllCaches()
    {
        init();
    }

    public synchronized void purge(IndexReader r)
    {
        for (Cache c : caches.values())
        {
            c.purge(r);
        }
    }

    public synchronized CacheEntry[] getCacheEntries()
    {
        List<CacheEntry> result = new ArrayList<CacheEntry>(17);
        for (final Map.Entry<Class<?>, Cache> cacheEntry : caches.entrySet())
        {
            final Cache cache = cacheEntry.getValue();
            final Class<?> cacheType = cacheEntry.getKey();
            synchronized (cache.readerCache)
            {
                for (final Map.Entry<Object, Map<Entry, Object>> readerCacheEntry : cache.readerCache.entrySet())
                {
                    final Object readerKey = readerCacheEntry.getKey();
                    if (readerKey == null)
                        continue;
                    final Map<Entry, Object> innerCache = readerCacheEntry.getValue();
                    for (final Map.Entry<Entry, Object> mapEntry : innerCache.entrySet())
                    {
                        Entry entry = mapEntry.getKey();
                        result.add(new CacheEntryImpl(readerKey, entry.field, cacheType, entry.custom, mapEntry
                                .getValue()));
                    }
                }
            }
        }
        return result.toArray(new CacheEntry[result.size()]);
    }

    private static final class CacheEntryImpl extends CacheEntry
    {
        private final Object   readerKey;
        private final String   fieldName;
        private final Class<?> cacheType;
        private final Object   custom;
        private final Object   value;

        CacheEntryImpl(Object readerKey, String fieldName, Class<?> cacheType, Object custom, Object value)
        {
            this.readerKey = readerKey;
            this.fieldName = fieldName;
            this.cacheType = cacheType;
            this.custom = custom;
            this.value = value;

            // :HACK: for testing.
            // if (null != locale || SortField.CUSTOM != sortFieldType) {
            // throw new RuntimeException("Locale/sortFieldType: " + this);
            // }

        }

        @Override
        public Object getReaderKey()
        {
            return readerKey;
        }

        @Override
        public String getFieldName()
        {
            return fieldName;
        }

        @Override
        public Class<?> getCacheType()
        {
            return cacheType;
        }

        @Override
        public Object getCustom()
        {
            return custom;
        }

        @Override
        public Object getValue()
        {
            return value;
        }
    }

    /**
     * Hack: When thrown from a Parser (NUMERIC_UTILS_* ones), this stops
     * processing terms and returns the current FieldCache array.
     */
    static final class StopFillCacheException extends RuntimeException
    {
    }

    final static IndexReader.ReaderFinishedListener purgeReader = new IndexReader.ReaderFinishedListener() {
                                                                   
                                                                    public void finished(IndexReader reader)
                                                                    {
                                                                        FieldCache.DEFAULT.purge(reader);
                                                                    }
                                                                };

    /** Expert: Internal cache. */
    abstract static class Cache
    {
        Cache()
        {
            this.wrapper = null;
        }

        Cache(FieldCache wrapper)
        {
            this.wrapper = wrapper;
        }

        final FieldCache                      wrapper;

        final Map<Object, Map<Entry, Object>> readerCache = new WeakHashMap<Object, Map<Entry, Object>>();

        protected abstract Object createValue(IndexReader reader, Entry key) throws IOException;

        /** Remove this reader from the cache, if present. */
        public void purge(IndexReader r)
        {
            Object readerKey = r.getCoreCacheKey();
            synchronized (readerCache)
            {
                readerCache.remove(readerKey);
            }
        }

        public Object get(IndexReader reader, Entry key) throws IOException
        {
            Map<Entry, Object> innerCache;
            Object value;
            final Object readerKey = reader.getCoreCacheKey();
            synchronized (readerCache)
            {
                innerCache = readerCache.get(readerKey);
                if (innerCache == null)
                {
                    // First time this reader is using FieldCache
                    innerCache = new HashMap<Entry, Object>();
                    readerCache.put(readerKey, innerCache);
                    reader.addReaderFinishedListener(purgeReader);
                    value = null;
                }
                else
                {
                    value = innerCache.get(key);
                }
                if (value == null)
                {
                    value = new CreationPlaceholder();
                    innerCache.put(key, value);
                }
            }
            if (value instanceof CreationPlaceholder)
            {
                synchronized (value)
                {
                    CreationPlaceholder progress = (CreationPlaceholder) value;
                    if (progress.value == null)
                    {
                        progress.value = createValue(reader, key);
                        synchronized (readerCache)
                        {
                            innerCache.put(key, progress.value);
                        }

                        // Only check if key.custom (the parser) is
                        // non-null; else, we check twice for a single
                        // call to FieldCache.getXXX
                        if (key.custom != null && wrapper != null)
                        {
                            final PrintStream infoStream = wrapper.getInfoStream();
                            if (infoStream != null)
                            {
                                printNewInsanity(infoStream, progress.value);
                            }
                        }
                    }
                    return progress.value;
                }
            }
            return value;
        }

        private void printNewInsanity(PrintStream infoStream, Object value)
        {
            final FieldCacheSanityChecker.Insanity[] insanities = FieldCacheSanityChecker.checkSanity(wrapper);
            for (int i = 0; i < insanities.length; i++)
            {
                final FieldCacheSanityChecker.Insanity insanity = insanities[i];
                final CacheEntry[] entries = insanity.getCacheEntries();
                for (int j = 0; j < entries.length; j++)
                {
                    if (entries[j].getValue() == value)
                    {
                        // OK this insanity involves our entry
                        infoStream.println("WARNING: new FieldCache insanity created\nDetails: " + insanity.toString());
                        infoStream.println("\nStack:\n");
                        new Throwable().printStackTrace(infoStream);
                        break;
                    }
                }
            }
        }
    }

    /** Expert: Every composite-key in the internal cache is of this type. */
    static class Entry
    {
        final String field; // which Fieldable
        final Object custom; // which custom comparator or parser

        /** Creates one of these objects for a custom comparator/parser. */
        Entry(String field, Object custom)
        {
            this.field = StringHelper.intern(field);
            this.custom = custom;
        }

        /** Two of these are equal iff they reference the same field and type. */
        @Override
        public boolean equals(Object o)
        {
            if (o instanceof Entry)
            {
                Entry other = (Entry) o;
                if (other.field == field)
                {
                    if (other.custom == null)
                    {
                        if (custom == null)
                            return true;
                    }
                    else if (other.custom.equals(custom))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        /** Composes a hashcode based on the field and type. */
        @Override
        public int hashCode()
        {
            return field.hashCode() ^ (custom == null ? 0 : custom.hashCode());
        }
    }

    // inherit javadocs
    public byte[] getBytes(IndexReader reader, String field) throws IOException
    {
        return getBytes(reader, field, null);
    }

    // inherit javadocs
    public byte[] getBytes(IndexReader reader, String field, ByteParser parser) throws IOException
    {
        return (byte[]) caches.get(Byte.TYPE).get(reader, new Entry(field, parser));
    }

    static final class ByteCache extends Cache
    {
        ByteCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            Entry entry = entryKey;
            String field = entry.field;
            ByteParser parser = (ByteParser) entry.custom;
            if (parser == null)
            {
                return wrapper.getBytes(reader, field, FieldCache.DEFAULT_BYTE_PARSER);
            }
            final byte[] retArray = new byte[reader.maxDoc()];
            TermDocs termDocs = reader.termDocs();
            TermEnum termEnum = reader.terms(new Term(field));
            try
            {
                do
                {
                    Term term = termEnum.term();
                    if (term == null || term.field() != field)
                        break;
                    byte termval = parser.parseByte(term.text());
                    termDocs.seek(termEnum);
                    while (termDocs.next())
                    {
                        retArray[termDocs.doc()] = termval;
                    }
                }
                while (termEnum.next());
            }
            catch (StopFillCacheException stop)
            {
            }
            finally
            {
                termDocs.close();
                termEnum.close();
            }
            return retArray;
        }
    }

    // inherit javadocs
    public short[] getShorts(IndexReader reader, String field) throws IOException
    {
        return getShorts(reader, field, null);
    }

    // inherit javadocs
    public short[] getShorts(IndexReader reader, String field, ShortParser parser) throws IOException
    {
        return (short[]) caches.get(Short.TYPE).get(reader, new Entry(field, parser));
    }

    static final class ShortCache extends Cache
    {
        ShortCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            Entry entry = entryKey;
            String field = entry.field;
            ShortParser parser = (ShortParser) entry.custom;
            if (parser == null)
            {
                return wrapper.getShorts(reader, field, FieldCache.DEFAULT_SHORT_PARSER);
            }
            final short[] retArray = new short[reader.maxDoc()];
            TermDocs termDocs = reader.termDocs();
            TermEnum termEnum = reader.terms(new Term(field));
            try
            {
                do
                {
                    Term term = termEnum.term();
                    if (term == null || term.field() != field)
                        break;
                    short termval = parser.parseShort(term.text());
                    termDocs.seek(termEnum);
                    while (termDocs.next())
                    {
                        retArray[termDocs.doc()] = termval;
                    }
                }
                while (termEnum.next());
            }
            catch (StopFillCacheException stop)
            {
            }
            finally
            {
                termDocs.close();
                termEnum.close();
            }
            return retArray;
        }
    }

    // inherit javadocs
    public int[] getInts(IndexReader reader, String field) throws IOException
    {
        return getInts(reader, field, null);
    }

    // inherit javadocs
    public int[] getInts(IndexReader reader, String field, IntParser parser) throws IOException
    {
        return (int[]) caches.get(Integer.TYPE).get(reader, new Entry(field, parser));
    }

    static final class IntCache extends Cache
    {
        IntCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            Entry entry = entryKey;
            String field = entry.field;
            IntParser parser = (IntParser) entry.custom;
            if (parser == null)
            {
                try
                {
                    return wrapper.getInts(reader, field, DEFAULT_INT_PARSER);
                }
                catch (NumberFormatException ne)
                {
                    return wrapper.getInts(reader, field, NUMERIC_UTILS_INT_PARSER);
                }
            }
            int[] retArray = null;
            TermDocs termDocs = reader.termDocs();
            TermEnum termEnum = reader.terms(new Term(field));
            try
            {
                do
                {
                    Term term = termEnum.term();
                    if (term == null || term.field() != field)
                        break;
                    int termval = parser.parseInt(term.text());
                    if (retArray == null) // late init
                        retArray = new int[reader.maxDoc()];
                    termDocs.seek(termEnum);
                    while (termDocs.next())
                    {
                        retArray[termDocs.doc()] = termval;
                    }
                }
                while (termEnum.next());
            }
            catch (StopFillCacheException stop)
            {
            }
            finally
            {
                termDocs.close();
                termEnum.close();
            }
            if (retArray == null) // no values
                retArray = new int[reader.maxDoc()];
            return retArray;
        }
    }

    // inherit javadocs
    public float[] getFloats(IndexReader reader, String field) throws IOException
    {
        return getFloats(reader, field, null);
    }

    // inherit javadocs
    public float[] getFloats(IndexReader reader, String field, FloatParser parser) throws IOException
    {

        return (float[]) caches.get(Float.TYPE).get(reader, new Entry(field, parser));
    }

    static final class FloatCache extends Cache
    {
        FloatCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            Entry entry = entryKey;
            String field = entry.field;
            FloatParser parser = (FloatParser) entry.custom;
            if (parser == null)
            {
                try
                {
                    return wrapper.getFloats(reader, field, DEFAULT_FLOAT_PARSER);
                }
                catch (NumberFormatException ne)
                {
                    return wrapper.getFloats(reader, field, NUMERIC_UTILS_FLOAT_PARSER);
                }
            }
            float[] retArray = null;
            TermDocs termDocs = reader.termDocs();
            TermEnum termEnum = reader.terms(new Term(field));
            try
            {
                do
                {
                    Term term = termEnum.term();
                    if (term == null || term.field() != field)
                        break;
                    float termval = parser.parseFloat(term.text());
                    if (retArray == null) // late init
                        retArray = new float[reader.maxDoc()];
                    termDocs.seek(termEnum);
                    while (termDocs.next())
                    {
                        retArray[termDocs.doc()] = termval;
                    }
                }
                while (termEnum.next());
            }
            catch (StopFillCacheException stop)
            {
            }
            finally
            {
                termDocs.close();
                termEnum.close();
            }
            if (retArray == null) // no values
                retArray = new float[reader.maxDoc()];
            return retArray;
        }
    }

    public long[] getLongs(IndexReader reader, String field) throws IOException
    {
        return getLongs(reader, field, null);
    }

    // inherit javadocs
    public long[] getLongs(IndexReader reader, String field, FieldCache.LongParser parser) throws IOException
    {
        return (long[]) caches.get(Long.TYPE).get(reader, new Entry(field, parser));
    }

    static final class LongCache extends Cache
    {
        LongCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entry) throws IOException
        {
            String field = entry.field;
            FieldCache.LongParser parser = (FieldCache.LongParser) entry.custom;
            if (parser == null)
            {
                try
                {
                    return wrapper.getLongs(reader, field, DEFAULT_LONG_PARSER);
                }
                catch (NumberFormatException ne)
                {
                    return wrapper.getLongs(reader, field, NUMERIC_UTILS_LONG_PARSER);
                }
            }
            long[] retArray = null;
            
            Collection<IColumn> fcEntries = getFieldCacheEntries(reader, field);

            try
            {
                for (IColumn col : fcEntries)
                {
                    if (col instanceof DeletedColumn)
                        continue;

                    int docId = CassandraUtils.readVInt(col.name());
                    long termval = parser.parseLong(ByteBufferUtil.string(col.value()));

                    if (retArray == null) // late init
                        retArray = new long[reader.maxDoc()];

                    retArray[docId] = termval;
                }
            }
            catch (StopFillCacheException e)
            {
            }
            
            
            if (retArray == null) // no values
                retArray = new long[reader.maxDoc()];
            return retArray;
        }
    }

    // inherit javadocs
    public double[] getDoubles(IndexReader reader, String field) throws IOException
    {
        return getDoubles(reader, field, null);
    }

    // inherit javadocs
    public double[] getDoubles(IndexReader reader, String field, FieldCache.DoubleParser parser) throws IOException
    {
        return (double[]) caches.get(Double.TYPE).get(reader, new Entry(field, parser));
    }

    static final class DoubleCache extends Cache
    {
        DoubleCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            Entry entry = entryKey;
            String field = entry.field;
            FieldCache.DoubleParser parser = (FieldCache.DoubleParser) entry.custom;
            if (parser == null)
            {
                try
                {
                    return wrapper.getDoubles(reader, field, DEFAULT_DOUBLE_PARSER);
                }
                catch (NumberFormatException ne)
                {
                    return wrapper.getDoubles(reader, field, NUMERIC_UTILS_DOUBLE_PARSER);
                }
            }
            double[] retArray = null;

            Collection<IColumn> fcEntries = getFieldCacheEntries(reader, field);

            try
            {
                for (IColumn col : fcEntries)
                {
                    if (col instanceof DeletedColumn)
                        continue;

                    int docId = CassandraUtils.readVInt(col.name());
                    double termval = parser.parseDouble(ByteBufferUtil.string(col.value()));

                    if (retArray == null) // late init
                        retArray = new double[reader.maxDoc()];

                    retArray[docId] = termval;
                }
            }
            catch (StopFillCacheException e)
            {
            }

            if (retArray == null) // no values
                retArray = new double[reader.maxDoc()];
            return retArray;
        }
    }

    // inherit javadocs
    public String[] getStrings(IndexReader reader, String field) throws IOException
    {
        return (String[]) caches.get(String.class).get(reader, new Entry(field, (Parser) null));
    }

    static final class StringCache extends Cache
    {
        StringCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            String field = StringHelper.intern(entryKey.field);
            final String[] retArray = new String[reader.maxDoc()];

            Collection<IColumn> fcEntries = getFieldCacheEntries(reader, field);

            for (IColumn col : fcEntries)
            {
                if (col instanceof DeletedColumn)
                    continue;

                int docId = CassandraUtils.readVInt(col.name());
                String val = ByteBufferUtil.string(col.value());

                retArray[docId] = val;
            }

            return retArray;
        }
    }

    // inherit javadocs
    public StringIndex getStringIndex(IndexReader reader, String field) throws IOException
    {
        return (StringIndex) caches.get(StringIndex.class).get(reader, new Entry(field, (Parser) null));
    }

    static final class StringIndexCache extends Cache
    {
        StringIndexCache(FieldCache wrapper)
        {
            super(wrapper);
        }

        @Override
        protected Object createValue(IndexReader reader, Entry entryKey) throws IOException
        {
            String field = StringHelper.intern(entryKey.field);
            final int[] retArray = new int[reader.maxDoc()];
            String[] mterms = new String[reader.maxDoc() + 1];
            int t = 0; // current term number

            mterms[t++] = null;

            Collection<IColumn> fcEntries = getFieldCacheEntries(reader, field);

            Map<String, Integer> uniqueStrings = new HashMap<String, Integer>();

            for (IColumn col : fcEntries)
            {
                if (col instanceof DeletedColumn)
                    continue;

                int docId = CassandraUtils.readVInt(col.name());
                String val = ByteBufferUtil.string(col.value());

                Integer idx = uniqueStrings.get(val);

                if (idx == null)
                {
                    uniqueStrings.put(val, t);

                    mterms[t] = val;
                    idx = t;

                    t++;
                }

                if (val == null)
                    retArray[docId] = 0;
                else
                    retArray[docId] = idx;
            }

            if (t == 0)
            {
                // if there are no terms, make the term array
                // have a single null entry
                mterms = new String[1];
            }
            else if (t < mterms.length)
            {
                // if there are less terms than documents,
                // trim off the dead array space
                String[] terms = new String[t];
                System.arraycopy(mterms, 0, terms, 0, t);
                mterms = terms;
            }

            StringIndex value = new StringIndex(retArray, mterms);
            return value;
        }
    }

    private static Collection<IColumn> getFieldCacheEntries(IndexReader indexReader, String field) throws IOException
    {

        String indexName = SolandraCoreContainer.coreInfo.get().indexName + "~"
                + SolandraCoreContainer.coreInfo.get().shard;

        byte[] indexNameBytes = indexName.getBytes("UTF-8");

        logger.info("Loading field cache from " + indexName + " " + field);

        ColumnParent fieldCacheParent = new ColumnParent(CassandraUtils.fieldCacheColumnFamily);
        ByteBuffer fieldCacheKey = CassandraUtils.hashKeyBytes(indexNameBytes, CassandraUtils.delimeterBytes, field
                .getBytes());

        List<Row> rows = CassandraUtils.robustRead(CassandraUtils.consistency, new SliceFromReadCommand(
                CassandraUtils.keySpace, fieldCacheKey, fieldCacheParent, ByteBufferUtil.EMPTY_BYTE_BUFFER,
                ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE));

        if (rows.isEmpty())
            throw new IOException("Field cache data missing");

        Row row = rows.get(0);
        if (row.cf == null)
            throw new IOException("Field cache data missing");

        return row.cf.getSortedColumns();
    }

    private volatile PrintStream infoStream;

    public void setInfoStream(PrintStream stream)
    {
        infoStream = stream;
    }

    public PrintStream getInfoStream()
    {
        return infoStream;
    }
}