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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
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

    private final String indexName;

    private boolean autoCommit;
    private static final ThreadLocal<Map<String,RowMutation>> mutationList = new ThreadLocal<Map<String,RowMutation>>();

    private Similarity similarity = Similarity.getDefault(); // how to
                                                             // normalize;
    private final ColumnPath docAllColumnPath;
    private static final Logger logger = Logger.getLogger(IndexWriter.class);

    public IndexWriter(String indexName) {

        this.indexName = indexName;
        autoCommit = true;
        docAllColumnPath = new ColumnPath(CassandraUtils.docColumnFamily);

    }

    @SuppressWarnings("unchecked")
    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        List<String> allIndexedTerms  = new ArrayList<String>();
        Map<String,byte[]> fieldCache = new HashMap<String,byte[]>(1024);
        
        
        // check for special field name
        String docId = doc.get(CassandraUtils.documentIdField);

        if (docId == null)
            docId = Long.toHexString((long) (System.nanoTime() + (Math.random() * System.nanoTime())));

        int position = 0;

        for (Fieldable field : (List<Fieldable>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term information per field
                Map<String, Map<String, List<Number>>> allTermInformation = new HashMap<String, Map<String, List<Number>>>();

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
                    String term = CassandraUtils.createColumnName(field.name(), termAttribute.term());

                    allIndexedTerms.add(term);

                    // fetch all collected information for this term
                    Map<String, List<Number>> termInfo = allTermInformation.get(term);

                    if (termInfo == null) {
                        termInfo = new HashMap<String, List<Number>>();
                        allTermInformation.put(term, termInfo);
                    }

                    // term frequency
                    {
                        List<Number> termFrequency = termInfo.get(CassandraUtils.termFrequencyKey);

                        if (termFrequency == null) {
                            termFrequency = new ArrayList<Number>();
                            termFrequency.add(new Integer(0));
                            termInfo.put(CassandraUtils.termFrequencyKey, termFrequency);
                        }

                        // increment
                        termFrequency.set(0, termFrequency.get(0).intValue() + 1);
                    }

                    // position vector
                    if (field.isStorePositionWithTermVector()) {
                        position += (posIncrAttribute.getPositionIncrement() - 1);

                        List<Number> positionVector = termInfo.get(CassandraUtils.positionVectorKey);

                        if (positionVector == null) {
                            positionVector = new ArrayList<Number>();
                            termInfo.put(CassandraUtils.positionVectorKey, positionVector);
                        }

                        positionVector.add(++position);
                    }

                    // term offsets
                    if (field.isStoreOffsetWithTermVector()) {

                        List<Number> offsetVector = termInfo.get(CassandraUtils.offsetVectorKey);
                        if (offsetVector == null) {
                            offsetVector = new ArrayList<Number>();
                            termInfo.put(CassandraUtils.offsetVectorKey, offsetVector);
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

                for (Map.Entry<String, Map<String, List<Number>>> term : allTermInformation.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all columns
                    // in a key/column family into memory
                    String key = indexName + CassandraUtils.delimeter + term.getKey();

                    // Mix in the norm for this field alongside each term
                    // more writes but faster on read side.
                    if (!field.getOmitNorms()) {
                        term.getValue().put(CassandraUtils.normsKey, bnorm);
                    }

                    CassandraUtils.addMutations(getMutationList(), CassandraUtils.termVecColumnFamily, docId.getBytes("UTF-8"), CassandraUtils.hashKey(key),
                            null, term.getValue());

                }
            }

            // Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized()) {
                String term = CassandraUtils.createColumnName(field.name(), field.stringValue());
                allIndexedTerms.add(term);

                String key = indexName + CassandraUtils.delimeter + term;

                Map<String, List<Number>> termMap = new HashMap<String, List<Number>>();
                termMap.put(CassandraUtils.termFrequencyKey, CassandraUtils.emptyArray);
                termMap.put(CassandraUtils.positionVectorKey, CassandraUtils.emptyArray);

                CassandraUtils.addMutations(getMutationList(), CassandraUtils.termVecColumnFamily, docId.getBytes("UTF-8"), CassandraUtils.hashKey(key), null,
                        termMap);

            }

            // Stores each field as a column under this doc key
            if (field.isStored()) {

                byte[] _value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes("UTF-8");

                // first byte flags if binary or not
                byte[] value = new byte[_value.length + 1];
                System.arraycopy(_value, 0, value, 0, _value.length);

                value[value.length - 1] = (byte) (field.isBinary() ? Byte.MAX_VALUE : Byte.MIN_VALUE);

                //logic to handle multiple fields w/ same name
                byte[] currentValue = fieldCache.get(field.name());
                if(currentValue == null){
                    fieldCache.put(field.name(), value);
                }else{
                    
                    // append new data
                    byte[] newValue = new byte[currentValue.length + CassandraUtils.delimeterBytes.length + value.length - 1];
                    System.arraycopy(currentValue, 0, newValue, 0, currentValue.length - 1);
                    System.arraycopy(CassandraUtils.delimeterBytes, 0, newValue, currentValue.length - 1, CassandraUtils.delimeterBytes.length);
                    System.arraycopy(value, 0, newValue, currentValue.length + CassandraUtils.delimeterBytes.length - 1, value.length);

                    fieldCache.put(field.name(), newValue);   
                }           
            }
        }

        
        String key = indexName + CassandraUtils.delimeter + docId;

        //Store each field as a column under this docId
        for(Map.Entry<String, byte[]> field : fieldCache.entrySet()){
            CassandraUtils.addMutations(getMutationList(), CassandraUtils.docColumnFamily, field.getKey().getBytes("UTF-8"), CassandraUtils.hashKey(key),
                field.getValue(), null);
        }
        
        // Finally, Store meta-data so we can delete this document
        CassandraUtils.addMutations(getMutationList(), CassandraUtils.docColumnFamily, CassandraUtils.documentMetaField.getBytes("UTF-8"), CassandraUtils
                .hashKey(key), CassandraUtils.toBytes(allIndexedTerms), null);

        if (autoCommit) {
            CassandraUtils.robustInsert(getMutationList());
        }
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {

        IndexReader reader = new IndexReader(indexName);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs results = searcher.search(query, 1000);

        for (int i = 0; i < results.totalHits; i++) {
            ScoreDoc doc = results.scoreDocs[i];

            String docId = reader.getDocumentId(doc.doc);
            
            deleteLucandraDocument(docId.getBytes("UTF-8"));
           
        }

    }

    @SuppressWarnings("unchecked")
    public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
        try {

            ColumnParent cp = new ColumnParent(CassandraUtils.termVecColumnFamily);
            String key = indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(term);

            ReadCommand rc = new SliceFromReadCommand(CassandraUtils.keySpace, CassandraUtils.hashKey(key), cp, new byte[] {}, new byte[] {}, false,
                    Integer.MAX_VALUE);

            List<Row> rows = StorageProxy.readProtocol(Arrays.asList(rc), ConsistencyLevel.ONE);

            // delete by documentId
            for (Row row : rows) {
                for (IColumn col : row.cf.getSortedColumns()) {
                    deleteLucandraDocument(col.name());
                }
            }

        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteLucandraDocument(byte[] docId) {

        String key = indexName + CassandraUtils.delimeter + new String(docId);

        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, CassandraUtils.hashKey(key), CassandraUtils.metaColumnPath, Arrays
                .asList(CassandraUtils.documentMetaFieldBytes));

        List<Row> rows = null;
        int attempts = 0;
        while (attempts++ < 10) {
            try {
                rows = StorageProxy.readProtocol(Arrays.asList(rc), ConsistencyLevel.ONE);
                break;
            } catch (IOException e1) {
               throw new RuntimeException(e1);
            } catch (UnavailableException e1) {
                
            } catch (TimeoutException e1) {
               
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                
            }
        }

        if(attempts >= 10)
            throw new RuntimeException("Read command failed after 10 attempts");
        
        if (rows.isEmpty())
            return; // nothing to delete

        List<String> terms;
        try {
            terms = (List<String>) CassandraUtils.fromBytes(rows.get(0).cf.getColumn(CassandraUtils.documentMetaFieldBytes).value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (String termStr : terms) {

            key = indexName + CassandraUtils.delimeter + termStr;

            CassandraUtils.addMutations(getMutationList(), CassandraUtils.termVecColumnFamily, docId, CassandraUtils.hashKey(key), null, null);
        }

        // finally delete ourselves
        String selfKey = indexName + CassandraUtils.delimeter + new String(docId);
        CassandraUtils.addMutations(getMutationList(), CassandraUtils.docColumnFamily, null, CassandraUtils.hashKey(selfKey), null, null);

        if (autoCommit)
            CassandraUtils.robustInsert(getMutationList());

    }

    public void updateDocument(Term updateTerm, Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        deleteDocuments(updateTerm);
        addDocument(doc, analyzer);

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
        if (!autoCommit)
            CassandraUtils.robustInsert(getMutationList());
    }

    private Map<String,RowMutation> getMutationList() {

        Map<String,RowMutation> list = mutationList.get();

        if (list == null) {
            list = new HashMap<String,RowMutation>(1024);
            mutationList.set(list);
        }

        return list;
    }

}
