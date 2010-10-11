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
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TopDocs;

public class IndexWriter {

    private String indexName;

    private boolean autoCommit;
    private static final ThreadLocal<Map<byte[], RowMutation>> mutationList = new ThreadLocal<Map<byte[], RowMutation>>();

    private Similarity similarity = Similarity.getDefault(); // how to normalize;
    private static final Logger logger = Logger.getLogger(IndexWriter.class);
    
    public IndexWriter(String indexName) {
        this.indexName = indexName;
        autoCommit = true;
    }

    public void addDocument(Document doc, Analyzer analyzer, int docNumber, String indexName) throws CorruptIndexException, IOException{
        this.indexName = indexName;      
        addDocument(doc, analyzer, docNumber);
    }
    
    @SuppressWarnings("unchecked")
    public void addDocument(Document doc, Analyzer analyzer, int docNumber) throws CorruptIndexException, IOException {

        List<Term> allIndexedTerms = new ArrayList<Term>();
        Map<String, byte[]> fieldCache = new HashMap<String, byte[]>(1024);

        //By default we don't handle indexSharding
        //We round robin replace the index      
        docNumber = docNumber % CassandraUtils.maxDocsPerShard;
        
       
        byte[] docId = CassandraUtils.writeVInt(docNumber);
        int position = 0;

        for (Fieldable field : (List<Fieldable>) doc.getFields()) {

            
            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term information per field
                Map<Term, Map<byte[], List<Number>>> allTermInformation = new ConcurrentSkipListMap<Term, Map<byte[], List<Number>>>();

                int lastOffset = 0;
                if (position > 0) {
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
                    posIncrAttribute = (PositionIncrementAttribute) tokens.addAttribute(PositionIncrementAttribute.class);

                TermAttribute termAttribute = (TermAttribute) tokens.addAttribute(TermAttribute.class);

                // store normalizations of field per term per document rather
                // than per field.
                // this adds more to write but less to read on other side
                Integer tokensInField = new Integer(0);

                while (tokens.incrementToken()) {
                    tokensInField++;
                    Term term = new Term(field.name(), termAttribute.term());

                    allIndexedTerms.add(term);

                    // fetch all collected information for this term
                    Map<byte[], List<Number>> termInfo = allTermInformation.get(term);

                    if (termInfo == null) {
                        termInfo = new ConcurrentSkipListMap<byte[], List<Number>>(FBUtilities.byteArrayComparator);
                        allTermInformation.put(term, termInfo);
                    }

                    // term frequency
                    {
                        List<Number> termFrequency = termInfo.get(CassandraUtils.termFrequencyKeyBytes);

                        if (termFrequency == null) {
                            termFrequency = new ArrayList<Number>();
                            termFrequency.add(new Integer(0));
                            termInfo.put(CassandraUtils.termFrequencyKeyBytes, termFrequency);
                        }

                        // increment
                        termFrequency.set(0, termFrequency.get(0).intValue() + 1);
                    }

                    // position vector
                    if (field.isStorePositionWithTermVector()) {
                        position += (posIncrAttribute.getPositionIncrement() - 1);

                        List<Number> positionVector = termInfo.get(CassandraUtils.positionVectorKeyBytes);

                        if (positionVector == null) {
                            positionVector = new ArrayList<Number>();
                            termInfo.put(CassandraUtils.positionVectorKeyBytes, positionVector);
                        }

                        positionVector.add(++position);
                    }

                    // term offsets
                    if (field.isStoreOffsetWithTermVector()) {

                        List<Number> offsetVector = termInfo.get(CassandraUtils.offsetVectorKeyBytes);
                        if (offsetVector == null) {
                            offsetVector = new ArrayList<Number>();
                            termInfo.put(CassandraUtils.offsetVectorKeyBytes, offsetVector);
                        }

                        offsetVector.add(lastOffset + offsetAttribute.startOffset());
                        offsetVector.add(lastOffset + offsetAttribute.endOffset());

                    }
                }

                List<Number> bnorm = null;
                if (!field.getOmitNorms()) {
                    bnorm = new ArrayList<Number>();
                    float norm = doc.getBoost();
                    norm *= field.getBoost();
                    norm *= similarity.lengthNorm(field.name(), tokensInField);
                    bnorm.add(Similarity.encodeNorm(norm));
                }

                for (Map.Entry<Term, Map<byte[], List<Number>>> term : allTermInformation.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all columns
                    // in a key/column family into memory
                    byte[] key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.getKey().field().getBytes(),
                            CassandraUtils.delimeterBytes, term.getKey().text().getBytes("UTF-8"));

                    byte[] termkey = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.getKey().field().getBytes());
                    
                    // Mix in the norm for this field alongside each term
                    // more writes but faster on read side.
                    if (!field.getOmitNorms()) {
                        term.getValue().put(CassandraUtils.normsKeyBytes, bnorm);
                    }

                    CassandraUtils.addMutations(getMutationList(), CassandraUtils.termVecColumnFamily, docId, key, null, term.getValue());
                    CassandraUtils.addMutations(getMutationList(), CassandraUtils.metaInfoColumnFamily, term.getKey().text().getBytes("UTF-8"), termkey, CassandraUtils.emptyByteArray, null);
                    
                }
            }

            // Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized()) {
                Term term = new Term(field.name(), field.stringValue());
                allIndexedTerms.add(term);

                byte[] key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, field.name().getBytes(),
                        CassandraUtils.delimeterBytes, field.stringValue().getBytes("UTF-8"));

                byte[] termkey = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, field.name().getBytes());
                
                
                Map<byte[], List<Number>> termMap = new ConcurrentSkipListMap<byte[], List<Number>>(FBUtilities.byteArrayComparator);
                termMap.put(CassandraUtils.termFrequencyKeyBytes, CassandraUtils.emptyArray);
                termMap.put(CassandraUtils.positionVectorKeyBytes, CassandraUtils.emptyArray);

                CassandraUtils.addMutations(getMutationList(), CassandraUtils.termVecColumnFamily, docId, key, null, termMap);
                CassandraUtils.addMutations(getMutationList(), CassandraUtils.metaInfoColumnFamily, field.stringValue().getBytes("UTF-8"), termkey, CassandraUtils.emptyByteArray, null);
            }

            // Stores each field as a column under this doc key
            if (field.isStored()) {

                byte[] _value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes("UTF-8");

                // first byte flags if binary or not
                byte[] value = new byte[_value.length + 1];
                System.arraycopy(_value, 0, value, 0, _value.length);

                value[value.length - 1] = (byte) (field.isBinary() ? Byte.MAX_VALUE : Byte.MIN_VALUE);

                // logic to handle multiple fields w/ same name
                byte[] currentValue = fieldCache.get(field.name());
                if (currentValue == null) {
                    fieldCache.put(field.name(), value);
                } else {

                    // append new data
                    byte[] newValue = new byte[currentValue.length + CassandraUtils.delimeterBytes.length + value.length - 1];
                    System.arraycopy(currentValue, 0, newValue, 0, currentValue.length - 1);
                    System.arraycopy(CassandraUtils.delimeterBytes, 0, newValue, currentValue.length - 1, CassandraUtils.delimeterBytes.length);
                    System.arraycopy(value, 0, newValue, currentValue.length + CassandraUtils.delimeterBytes.length - 1, value.length);

                    fieldCache.put(field.name(), newValue);
                }
            }
        }

        byte[] key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, Integer.toHexString(docNumber).getBytes());

        // Store each field as a column under this docId
        for (Map.Entry<String, byte[]> field : fieldCache.entrySet()) {
            CassandraUtils.addMutations(getMutationList(), CassandraUtils.docColumnFamily, field.getKey().getBytes("UTF-8"), key, field.getValue(), null);
        }

        // Finally, Store meta-data so we can delete this document
        CassandraUtils.addMutations(getMutationList(), CassandraUtils.docColumnFamily, CassandraUtils.documentMetaFieldBytes, key, CassandraUtils
                .toBytes(allIndexedTerms), null);

        if (autoCommit) {
            CassandraUtils.robustInsert(Arrays.asList(getMutationList().values().toArray(new RowMutation[]{})), ConsistencyLevel.ONE);
            getMutationList().clear();
        }
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {

        IndexReader reader = new IndexReader(indexName);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs results = searcher.search(query, 1000);

        for (int i = 0; i < results.totalHits; i++) {
            ScoreDoc doc = results.scoreDocs[i];

            byte[] docId = Integer.toHexString(doc.doc).getBytes();

            deleteLucandraDocument(docId);

        }

    }

    public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
        try {

            ColumnParent cp = new ColumnParent(CassandraUtils.termVecColumnFamily);

            byte[] key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.field().getBytes(),
                    CassandraUtils.delimeterBytes, term.text().getBytes("UTF-8"));

            ReadCommand rc = new SliceFromReadCommand(CassandraUtils.keySpace, key, cp, new byte[] {}, new byte[] {}, false, Integer.MAX_VALUE);

            List<Row> rows = StorageProxy.readProtocol(Arrays.asList(rc), ConsistencyLevel.ONE);

            // delete by documentId
            for (Row row : rows) {
                if(row.cf != null){
                    Collection<IColumn> columns = row.cf.getSortedColumns();
                
                    for (IColumn col : columns) {
                        deleteLucandraDocument(Integer.toHexString(CassandraUtils.readVInt(col.name())).getBytes());
                    }
                }
            }

        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteLucandraDocument(byte[] docId) {

        byte[] key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, docId);

        List<Row> rows = CassandraUtils.robustGet(key, CassandraUtils.metaColumnPath, Arrays.asList(CassandraUtils.documentMetaFieldBytes), ConsistencyLevel.ONE);
        
        if (rows.isEmpty())
            return; // nothing to delete

        List<Term> terms;
        try {
            terms = (List<Term>) CassandraUtils.fromBytes(rows.get(0).cf.getColumn(CassandraUtils.documentMetaFieldBytes).value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (Term term : terms) {

            try {
                key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.field().getBytes(), CassandraUtils.delimeterBytes,
                        term.text().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("JVM doesn't support UTF-8", e);
            }

            CassandraUtils.addMutations(getMutationList(), CassandraUtils.termVecColumnFamily, docId, key, null, null);
        }

        // finally delete ourselves
        byte[] selfKey = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, docId);
        CassandraUtils.addMutations(getMutationList(), CassandraUtils.docColumnFamily, null, selfKey, null, null);

        if (autoCommit){
            CassandraUtils.robustInsert(Arrays.asList(getMutationList().values().toArray(new RowMutation[]{})), ConsistencyLevel.ONE);
            getMutationList().clear();
        }
    }

    public void updateDocument(Term updateTerm, Document doc, Analyzer analyzer, int docNumber) throws CorruptIndexException, IOException {

        deleteDocuments(updateTerm);
        addDocument(doc, analyzer, docNumber);

    }

    public int docCount() {

        throw new RuntimeException("not supported");

    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public void commit() {
        if (!autoCommit){
            CassandraUtils.robustInsert(Arrays.asList(getMutationList().values().toArray(new RowMutation[]{})), ConsistencyLevel.ONE);
            getMutationList().clear();
        }
    }

    private Map<byte[], RowMutation> getMutationList() {

        Map<byte[], RowMutation> list = mutationList.get();

        if (list == null) {
            list = new ConcurrentSkipListMap<byte[], RowMutation>(FBUtilities.byteArrayComparator);
            mutationList.set(list);
        }

        return list;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

}
