/**
 * Copyright T Jake Luciani
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
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import lucandra.cluster.CassandraIndexManager;

import com.google.common.collect.MapMaker;

import org.apache.cassandra.db.*;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

public class IndexWriter
{
    private static final ConcurrentMap<String, Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>>> mutationList = new MapMaker()
                                                                                                                           .makeMap();
    private Similarity                                                                                similarity   = Similarity
                                                                                                                           .getDefault();
    private static final Logger                                                                       logger       = Logger
                                                                                                                           .getLogger(IndexWriter.class);

    public IndexWriter()
    {

    }

    @SuppressWarnings("unchecked")
    public void addDocument(String indexName, Document doc, Analyzer analyzer, int docNumber, boolean autoCommit,
            RowMutation rms[]) throws CorruptIndexException, IOException
    {

        Map<ByteBuffer, RowMutation> workingMutations = new HashMap<ByteBuffer, RowMutation>();

        byte[] indexNameBytes = indexName.getBytes("UTF-8");
        ByteBuffer indexTermsKey = CassandraUtils.hashKeyBytes(indexNameBytes, CassandraUtils.delimeterBytes, "terms"
                .getBytes("UTF-8"));

        List<Term> allIndexedTerms = new ArrayList<Term>();
        Map<String, byte[]> fieldCache = new HashMap<String, byte[]>(1024);

        // By default we don't handle indexSharding
        // We round robin replace the index
        docNumber = docNumber % CassandraIndexManager.maxDocsPerShard;

        ByteBuffer docId = ByteBuffer.wrap(CassandraUtils.writeVInt(docNumber));
        int position = 0;

        for (Fieldable field : (List<Fieldable>) doc.getFields())
        {

            // Indexed field
            if (field.isIndexed() && field.isTokenized())
            {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null)
                {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term information per field
                Map<Term, Map<ByteBuffer, List<Number>>> allTermInformation = new HashMap<Term, Map<ByteBuffer, List<Number>>>();

                int lastOffset = 0;
                if (position > 0)
                {
                    position += analyzer.getPositionIncrementGap(field.name());
                }

                // Build the termPositions vector for all terms

                tokens.reset(); // reset the TokenStream to the first token

                // set up token attributes we are working on

                // offsets
                OffsetAttribute offsetAttribute = null;
                if (field.isStoreOffsetWithTermVector())
                    offsetAttribute = (OffsetAttribute) tokens.addAttribute(OffsetAttribute.class);

                // positions
                PositionIncrementAttribute posIncrAttribute = null;
                if (field.isStorePositionWithTermVector())
                    posIncrAttribute = (PositionIncrementAttribute) tokens
                            .addAttribute(PositionIncrementAttribute.class);

                TermAttribute termAttribute = (TermAttribute) tokens.addAttribute(TermAttribute.class);

                // store normalizations of field per term per document rather
                // than per field.
                // this adds more to write but less to read on other side
                Integer tokensInField = new Integer(0);

                while (tokens.incrementToken())
                {
                    tokensInField++;
                    Term term = new Term(field.name(), termAttribute.term());

                    allIndexedTerms.add(term);

                    // fetch all collected information for this term
                    Map<ByteBuffer, List<Number>> termInfo = allTermInformation.get(term);

                    if (termInfo == null)
                    {
                        termInfo = new HashMap<ByteBuffer, List<Number>>();
                        allTermInformation.put(term, termInfo);
                    }

                    // term frequency
                    {
                        List<Number> termFrequency = termInfo.get(CassandraUtils.termFrequencyKeyBytes);

                        if (termFrequency == null)
                        {
                            termFrequency = new ArrayList<Number>();
                            termFrequency.add(new Integer(0));
                            termInfo.put(CassandraUtils.termFrequencyKeyBytes, termFrequency);
                        }

                        // increment
                        termFrequency.set(0, termFrequency.get(0).intValue() + 1);
                    }

                    // position vector
                    if (field.isStorePositionWithTermVector())
                    {
                        position += (posIncrAttribute.getPositionIncrement() - 1);

                        List<Number> positionVector = termInfo.get(CassandraUtils.positionVectorKeyBytes);

                        if (positionVector == null)
                        {
                            positionVector = new ArrayList<Number>();
                            termInfo.put(CassandraUtils.positionVectorKeyBytes, positionVector);
                        }

                        positionVector.add(++position);
                    }

                    // term offsets
                    if (field.isStoreOffsetWithTermVector())
                    {

                        List<Number> offsetVector = termInfo.get(CassandraUtils.offsetVectorKeyBytes);
                        if (offsetVector == null)
                        {
                            offsetVector = new ArrayList<Number>();
                            termInfo.put(CassandraUtils.offsetVectorKeyBytes, offsetVector);
                        }

                        offsetVector.add(lastOffset + offsetAttribute.startOffset());
                        offsetVector.add(lastOffset + offsetAttribute.endOffset());

                    }
                }

                List<Number> bnorm = null;
                if (!field.getOmitNorms())
                {
                    bnorm = new ArrayList<Number>();
                    float norm = doc.getBoost();
                    norm *= field.getBoost();
                    norm *= similarity.lengthNorm(field.name(), tokensInField);
                    bnorm.add(Similarity.encodeNorm(norm));
                }

                for (Map.Entry<Term, Map<ByteBuffer, List<Number>>> term : allTermInformation.entrySet())
                {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all columns
                    // in a key/column family into memory
                    ByteBuffer key = CassandraUtils.hashKeyBytes(indexNameBytes, CassandraUtils.delimeterBytes, term
                            .getKey().field().getBytes("UTF-8"), CassandraUtils.delimeterBytes, term.getKey().text().getBytes(
                            "UTF-8"));

                    ByteBuffer termkey = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"),
                            CassandraUtils.delimeterBytes, term.getKey().field().getBytes("UTF-8"));

                    // Mix in the norm for this field alongside each term
                    // more writes but faster on read side.
                    if (!field.getOmitNorms())
                    {
                        term.getValue().put(CassandraUtils.normsKeyBytes, bnorm);
                    }

                    CassandraUtils.addMutations(workingMutations, CassandraUtils.termVecColumnFamily, docId, key,
                            new LucandraTermInfo(docNumber, term.getValue()).serialize());

                    // Store all terms under a row
                    CassandraUtils.addMutations(workingMutations, CassandraUtils.metaInfoColumnFamily, CassandraUtils
                            .createColumnName(term.getKey()), indexTermsKey, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                }
            }

            // Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized())
            {
                Term term = new Term(field.name(), field.stringValue());
                allIndexedTerms.add(term);

                ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes, field
                        .name().getBytes("UTF-8"), CassandraUtils.delimeterBytes, field.stringValue().getBytes("UTF-8"));

                Map<ByteBuffer, List<Number>> termMap = new ConcurrentSkipListMap<ByteBuffer, List<Number>>();
                termMap.put(CassandraUtils.termFrequencyKeyBytes, CassandraUtils.emptyArray);
                termMap.put(CassandraUtils.positionVectorKeyBytes, CassandraUtils.emptyArray);

                CassandraUtils.addMutations(workingMutations, CassandraUtils.termVecColumnFamily, docId, key,
                        new LucandraTermInfo(docNumber, termMap).serialize());

                // Store all terms under a row
                CassandraUtils.addMutations(workingMutations, CassandraUtils.metaInfoColumnFamily, CassandraUtils
                        .createColumnName(field), indexTermsKey, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            }

            // Stores each field as a column under this doc key
            if (field.isStored())
            {

                byte[] _value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes("UTF-8");

                // first byte flags if binary or not
                byte[] value = new byte[_value.length + 1];
                System.arraycopy(_value, 0, value, 0, _value.length);

                value[value.length - 1] = (byte) (field.isBinary() ? Byte.MAX_VALUE : Byte.MIN_VALUE);

                // logic to handle multiple fields w/ same name
                byte[] currentValue = fieldCache.get(field.name());
                if (currentValue == null)
                {
                    fieldCache.put(field.name(), value);
                }
                else
                {

                    // append new data
                    byte[] newValue = new byte[currentValue.length + CassandraUtils.delimeterBytes.length
                            + value.length - 1];
                    System.arraycopy(currentValue, 0, newValue, 0, currentValue.length - 1);
                    System.arraycopy(CassandraUtils.delimeterBytes, 0, newValue, currentValue.length - 1,
                            CassandraUtils.delimeterBytes.length);
                    System.arraycopy(value, 0, newValue,
                            currentValue.length + CassandraUtils.delimeterBytes.length - 1, value.length);

                    fieldCache.put(field.name(), newValue);
                }
            }
        }

        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes, Integer
                .toHexString(docNumber).getBytes("UTF-8"));

        // Store each field as a column under this docId
        for (Map.Entry<String, byte[]> field : fieldCache.entrySet())
        {
            CassandraUtils.addMutations(workingMutations, CassandraUtils.docColumnFamily, field.getKey().getBytes(
                    "UTF-8"), key, field.getValue());
        }

        // Finally, Store meta-data so we can delete this document
        CassandraUtils.addMutations(workingMutations, CassandraUtils.docColumnFamily,
                CassandraUtils.documentMetaFieldBytes, key, CassandraUtils.toBytes(allIndexedTerms));

        if (rms != null)
        {
            Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>> mutationQ = getMutationQueue(indexName);

            List<RowMutation> rows = new ArrayList(Arrays.asList(rms));
            rows.addAll(workingMutations.values());

            mutationQ.right.addAll(rows);
        }
        else
        {
            appendMutations(indexName, workingMutations);
        }

        if (autoCommit)
            commit(indexName, false);
    }

    public void deleteDocuments(String indexName, Query query, boolean autoCommit) throws CorruptIndexException,
            IOException
    {

        IndexReader reader = new IndexReader(indexName);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs results = searcher.search(query, 1000);

        for (int i = 0; i < results.totalHits; i++)
        {
            ScoreDoc doc = results.scoreDocs[i];

            deleteLucandraDocument(indexName, doc.doc, autoCommit);
        }

    }

    public void deleteDocuments(String indexName, Term term, boolean autoCommit) throws CorruptIndexException,
            IOException
    {
        ColumnParent cp = new ColumnParent(CassandraUtils.termVecColumnFamily);

        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes, term
                .field().getBytes("UTF-8"), CassandraUtils.delimeterBytes, term.text().getBytes("UTF-8"));

        ReadCommand rc = new SliceFromReadCommand(CassandraUtils.keySpace, key, cp, ByteBufferUtil.EMPTY_BYTE_BUFFER,
                ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);

        List<Row> rows = CassandraUtils.robustRead(CassandraUtils.consistency, rc);

        // delete by documentId
        for (Row row : rows)
        {
            if (row.cf != null)
            {
                Collection<IColumn> columns = row.cf.getSortedColumns();
                
                for (IColumn col : columns)
                {
                    deleteLucandraDocument(indexName, CassandraUtils.readVInt(col.name()), autoCommit);
                }                 
            }
        }
    
    }

    private void deleteLucandraDocument(String indexName, int docNumber, boolean autoCommit) throws IOException
    {

        Map<ByteBuffer, RowMutation> workingMutations = new HashMap<ByteBuffer, RowMutation>();

        byte[] docId = Integer.toHexString(docNumber).getBytes("UTF-8");
        byte[] indexNameBytes = indexName.getBytes("UTF-8");

        ByteBuffer key = CassandraUtils.hashKeyBytes(indexNameBytes, CassandraUtils.delimeterBytes, docId);

        List<Row> rows = CassandraUtils.robustRead(key, CassandraUtils.metaColumnPath, Arrays
                .asList(CassandraUtils.documentMetaFieldBytes), CassandraUtils.consistency);

        if (rows.isEmpty() || rows.get(0).cf == null)
            return; // nothing to delete

        IColumn metaCol = rows.get(0).cf.getColumn(CassandraUtils.documentMetaFieldBytes);
        if (metaCol == null)
            return;

        List<Term> terms;
        try
        {
            terms = (List<Term>) CassandraUtils.fromBytes(metaCol.value());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        for (Term term : terms)
        {

            try
            {
                key = CassandraUtils.hashKeyBytes(indexNameBytes, CassandraUtils.delimeterBytes, term.field()
                        .getBytes("UTF-8"), CassandraUtils.delimeterBytes, term.text().getBytes("UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("JVM doesn't support UTF-8", e);
            }

            CassandraUtils.addMutations(workingMutations, CassandraUtils.termVecColumnFamily, CassandraUtils
                    .writeVInt(docNumber), key, (ByteBuffer) null);
        }

        // finally delete ourselves
        ByteBuffer selfKey = CassandraUtils.hashKeyBytes(indexNameBytes, CassandraUtils.delimeterBytes, docId);
        CassandraUtils.addMutations(workingMutations, CassandraUtils.docColumnFamily, (ByteBuffer) null, selfKey,
                (ByteBuffer) null);

        if(logger.isDebugEnabled())
            logger.debug("Deleted all terms for: " + docNumber);

        appendMutations(indexName, workingMutations);

        if (autoCommit)
            commit(indexName, false);
    }

    public void updateDocument(String indexName, Term updateTerm, Document doc, Analyzer analyzer, int docNumber,
            boolean autoCommit) throws CorruptIndexException, IOException
    {

        deleteDocuments(indexName, updateTerm, autoCommit);
        addDocument(indexName, doc, analyzer, docNumber, autoCommit, null);

    }

    public int docCount()
    {

        throw new RuntimeException("not supported");

    }

    // write completed mutations
    public void commit(String indexName, boolean blocked)
    {

        Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>> mutationQ = getMutationQueue(indexName);

        boolean success = false;

        List<RowMutation> rows = new ArrayList<RowMutation>();

        // Take and write
        try
        {
            while (blocked && mutationQ.left.get() > 0)
            {
                Thread.sleep(20);
            }

            // marked active write
            mutationQ.left.incrementAndGet();

            mutationQ.right.drainTo(rows);

            if (rows.isEmpty())
            {
                if(logger.isDebugEnabled())
                    logger.debug("Nothing to write for :" + indexName);
                return;
            }

            CassandraUtils.robustInsert(CassandraUtils.consistency, rows.toArray(new RowMutation[] {}));

            success = true;
        }
        catch (InterruptedException e)
        {
            // handled below
        }
        finally
        {

            // If write failed, add them back for another attempt
            if (!success)
            {
                if (rows != null)
                    mutationQ.right.addAll(rows);
            }
            else
            {
                if(logger.isDebugEnabled())
                    logger.debug("wrote " + rows.size());
            }

            // Mark we are done.
            mutationQ.left.decrementAndGet();
        }

    }

    // append complete mutations to the list
    private void appendMutations(String indexName, Map<ByteBuffer, RowMutation> mutations)
    {

        Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>> mutationQ = getMutationQueue(indexName);

        mutationQ.right.addAll(mutations.values());
    }

    private Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>> getMutationQueue(String indexName)
    {

        Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>> mutationQ = mutationList.get(indexName);

        if (mutationQ == null)
        {
            mutationQ = new Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>>(new AtomicInteger(0),
                    new LinkedBlockingQueue<RowMutation>());
            Pair<AtomicInteger, LinkedBlockingQueue<RowMutation>> liveQ = mutationList
                    .putIfAbsent(indexName, mutationQ);

            if (liveQ != null)
                mutationQ = liveQ;
        }

        return mutationQ;
    }
}
