/**
 * Copyright 2009 T Jake Luciani
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/**
 * 
 * @author jake
 * 
 */
public class LucandraTermEnum extends TermEnum
{

    private final IndexReader                    indexReader;
    private final String                         indexName;

    private int                                  termPosition;
    private Term[]                               termBuffer;
    private SortedMap<Term, Collection<IColumn>> termDocFreqBuffer;
    private SortedMap<Term, Collection<IColumn>> termCache;
    private Map<Term, IColumn[]>                 termDocsCache;

    // number of sequential terms to read initially
    private final int                            maxInitSize    = 2;
    private final int                            maxChunkSize   = 1024;
    private int                                  actualInitSize = -1;
    private Term                                 initTerm       = null;
    private Term                                 chunkBoundryTerm;
    private String                               currentField   = null;
    private int                                  chunkCount     = 0;

    private final Term                           finalTerm      = new Term(CassandraUtils.delimeter,
                                                                        CassandraUtils.finalToken);

    private static final Logger                  logger         = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader)
    {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
        this.termPosition = 0;
    }

    public boolean skipTo(Term term) throws IOException
    {

        if (term == null)
            return false;

        loadTerms(term);

        currentField = term.field();

        return termBuffer.length == 0 ? false : true;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public int docFreq()
    {
        return termDocFreqBuffer.size();
    }

    @Override
    public boolean next() throws IOException
    {

        if (termBuffer == null)
        {
            skipTo(new Term(""));
        }

        termPosition++;

        boolean hasNext = termPosition < termBuffer.length;

        if (hasNext && termBuffer[termPosition].equals(finalTerm))
        {
            termPosition++;
            hasNext = termPosition < termBuffer.length;
        }

        if (!hasNext)
        {

            // if we've already done init try grabbing more
            if ((chunkCount == 1 && actualInitSize == maxInitSize)
                    || (chunkCount > 1 && actualInitSize == maxChunkSize))
            {
                loadTerms(chunkBoundryTerm);
                hasNext = termBuffer.length > 0;
            }
            else if ((chunkCount == 1 && actualInitSize < maxInitSize)
                    || (chunkCount > 1 && actualInitSize < maxChunkSize))
            {
                hasNext = false;

                loadTerms(initTerm); // start over at top (for facets)
            }

            termPosition = 0;
        }

        return hasNext;
    }

    @Override
    public Term term()
    {
        return termBuffer[termPosition];
    }

    private void loadTerms(Term skipTo)
    {

        if (initTerm == null)
            initTerm = skipTo;

        ByteBuffer fieldKey = CassandraUtils.hashKeyBytes(indexName.getBytes(), 
                                                          CassandraUtils.delimeterBytes, 
                                                          skipTo.field().getBytes());

        // chose starting term
        ByteBuffer startTerm;
        try
        {
            startTerm = ByteBuffer.wrap(skipTo.text().getBytes("UTF-8"));

        }
        catch (UnsupportedEncodingException e1)
        {
            throw new RuntimeException("JVM doesn't support UTF-8", e1);
        }

        // ending term. the initial query we don't care since
        // we only pull 2 terms, also we don't
        ByteBuffer endTerm = FBUtilities.EMPTY_BYTE_BUFFER;

        // The boundary condition for this search. currently the field.
        ByteBuffer boundryTerm = CassandraUtils.finalTokenBytes;

        if ((!skipTo.equals(chunkBoundryTerm) || termPosition == 0) && termCache != null)
        {
            termDocFreqBuffer = termCache.subMap(skipTo, termCache.lastKey());
        }
        else
        {
            termDocFreqBuffer = null;
        }

        if (termDocFreqBuffer != null)
        {

            termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
            termPosition = 0;

            logger.debug("Found " + startTerm + " in cache");
            return;
        }
        else if (chunkCount > 1 && actualInitSize < maxChunkSize)
        {

            // include last term
            if (skipTo.equals(chunkBoundryTerm) && termCache.containsKey(skipTo))
            {
                termBuffer = new Term[] { skipTo };
                termDocFreqBuffer = termCache.subMap(skipTo, termCache.lastKey());
            }
            else
            {
                termBuffer = new Term[] {};
            }

            termPosition = 0;
            return; // done!
        }

        chunkCount++;

        // The first time we grab just a few keys
        int count = maxInitSize;

        // otherwise we grab all the rest of the keys
        if (chunkBoundryTerm != null)
        {
            count = maxChunkSize;
            try
            {
                startTerm = ByteBuffer.wrap(chunkBoundryTerm.text().getBytes("UTF-8"));

            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("JVM doesn't support UTF-8", e);
            }

            // After first pass use the boundary term, since we know on pass 2
            // we are using the OPP
            endTerm = boundryTerm;

        }

        long start = System.currentTimeMillis();

        termDocFreqBuffer = new TreeMap<Term, Collection<IColumn>>();

        ColumnParent columnFamily = new ColumnParent(CassandraUtils.metaInfoColumnFamily);

        List<Row> rows = CassandraUtils.robustRead(ConsistencyLevel.ONE, 
                new SliceFromReadCommand(CassandraUtils.keySpace, fieldKey, columnFamily, startTerm, endTerm, false, count));

        ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily);

        if (rows == null || rows.size() != 1)
            throw new RuntimeException("Missing field meta info");

        // Collect read commands
        Collection<IColumn> columns;
        if (rows.get(0).cf == null)
            columns = new ArrayList<IColumn>();
        else
            columns = rows.get(0).cf.getSortedColumns();

        List<ReadCommand> reads = new ArrayList<ReadCommand>(columns.size());
        for (IColumn column : columns)
        {

            ByteBuffer rowKey = CassandraUtils.hashKeyBytes(indexName.getBytes(), 
                                                            CassandraUtils.delimeterBytes, 
                                                            skipTo.field().getBytes(),
                                                            CassandraUtils.delimeterBytes, 
                                                            ByteBufferUtil.string(column.name()).getBytes());

            if(logger.isDebugEnabled())
                logger.debug("scanning row: "+ByteBufferUtil.string(rowKey));
            
            
            reads.add((ReadCommand) new SliceFromReadCommand(CassandraUtils.keySpace, rowKey, columnParent,
                    FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE));
        }

        rows = CassandraUtils.robustRead(ConsistencyLevel.ONE, reads.toArray(new ReadCommand[]{}));
   

        // term to start with next time
        actualInitSize = rows.size();
        if (logger.isDebugEnabled())
        {
            logger.debug("Found " + rows.size() + " keys in range:" + ByteBufferUtil.string(startTerm) + " to "
                    + ByteBufferUtil.string(endTerm) + " in " + (System.currentTimeMillis() - start) + "ms");

        }

        if (actualInitSize > 0)
        {
            for (Row row : rows)
            {

                if (row.cf == null)
                    continue;

                String key;
                try
                {
                    key = new String(row.key.key.array(), row.key.key.position() + row.key.key.arrayOffset(),
                            row.key.key.remaining(), "UTF-8");
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new RuntimeException("This JVM doesn't support UTF-8");
                }

                // term keys look like wikipedia/body/wiki
                String termStr = key.substring(key.indexOf(CassandraUtils.delimeter)
                        + CassandraUtils.delimeter.length());
                Term term = CassandraUtils.parseTerm(termStr);

                columns = row.cf.getSortedColumns();

                if(logger.isDebugEnabled())
                    logger.debug(term + " has " + columns.size());

                // check for tombstone keys or incorrect keys (from RP)
                // and verify this is from the correct index
                try
                {
                    if (columns.size() > 0
                            && term.field().equals(skipTo.field())
                            && ByteBufferUtil.compareUnsigned(row.key.key, 
                                    CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.field().getBytes(),
                                    CassandraUtils.delimeterBytes, term.text().getBytes("UTF-8"))) == 0)
                    {

                        if (!columns.iterator().next().isMarkedForDelete()){
                            if(logger.isDebugEnabled())
                                logger.debug("saving row: "+ByteBufferUtil.string(row.key.key));
                                
                            termDocFreqBuffer.put(term, columns);
                        }
                    }else{
                        logger.debug("Skipped column");
                    }
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new RuntimeException("JVM doesn't support UTF-8", e);
                }
            }

            if (!termDocFreqBuffer.isEmpty())
            {
                chunkBoundryTerm = termDocFreqBuffer.lastKey();
            }
        }

        // add a final key (excluded in submap below)
        termDocFreqBuffer.put(finalTerm, null);

        // put in cache
        for (Term termKey : termDocFreqBuffer.keySet())
        {

            if (termCache == null)
            {
                termCache = termDocFreqBuffer;
            }
            else
            {
                termCache.putAll(termDocFreqBuffer);
            }

            indexReader.addTermEnumCache(termKey, this);
        }

        // cache the initial term too
        indexReader.addTermEnumCache(skipTo, this);

        termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});

        termPosition = 0;

        long end = System.currentTimeMillis();

        if (logger.isDebugEnabled())
        {
            logger.debug("loadTerms: " + ByteBufferUtil.string(startTerm) + "(" + termBuffer.length + ") took "
                    + (end - start) + "ms");

        }
    }

    void loadFilteredTerms(Term term, List<ByteBuffer> docNums)
    {
        long start = System.currentTimeMillis();
        ColumnParent parent = new ColumnParent();
        parent.setColumn_family(CassandraUtils.termVecColumnFamily);

        ByteBuffer key;
        try
        {
            key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.field()
                    .getBytes(), CassandraUtils.delimeterBytes, term.text().getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException e2)
        {
            throw new RuntimeException("JVM doesn't support UTF-8", e2);
        }

        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, key, parent, docNums);

        List<Row> rows = CassandraUtils.robustRead(ConsistencyLevel.ONE, rc);

        termBuffer = new Term[0];

        if (rows != null && rows.size() > 0 && rows.get(0) != null && rows.get(0).cf != null
                && rows.get(0).cf.getSortedColumns() != null && rows.get(0).cf.getSortedColumns().size() > 0)
        {
            termBuffer = new Term[1];
            termBuffer[0] = term;
            termDocFreqBuffer = new TreeMap<Term, Collection<IColumn>>();
            termDocFreqBuffer.put(term, rows.get(0).cf.getSortedColumns());
        }
        long end = System.currentTimeMillis();
        logger.debug("loadFilterdTerms: " + term + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }

    public final IColumn[] getTermDocFreq()
    {
        if (termBuffer.length == 0)
            return null;

        Term term = termBuffer[termPosition];

        // Memoize
        IColumn[] docIds = null;

        // if(termDocsCache != null)
        // docIds = termDocsCache.get(term);

        // if(docIds != null)
        // return docIds;

        Collection<IColumn> termDocs = termDocFreqBuffer.get(term);

        // set normalizations
        indexReader.addDocumentNormalizations(termDocs, currentField);

        docIds = termDocs.toArray(new IColumn[] {});

        // if(termDocsCache == null)
        // termDocsCache = new HashMap<Term,IColumn[]>();

        // termDocsCache.put(term, docIds);

        return docIds;
    }

    public Set<Term> getCachedTerms()
    {
        return termCache.keySet();
    }

}
