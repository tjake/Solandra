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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.NotFoundException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.thrift.TException;

public class IndexWriter {

    private final String indexName;
    private final Cassandra.Client client;
    private final ColumnPath docAllColumnPath = new ColumnPath(CassandraUtils.docColumnFamily, null, null);
    private final ColumnPath metaColumnPath = new ColumnPath(CassandraUtils.docColumnFamily, null, CassandraUtils.documentMetaField.getBytes());

    private static final Logger logger = Logger.getLogger(IndexWriter.class);

    public IndexWriter(String indexName, Cassandra.Client client) {

        this.indexName = indexName;
        this.client = client;

    }

    @SuppressWarnings("unchecked")
    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        Token token = new Token();
        List<String> allIndexedTerms = new ArrayList<String>();
        
        
        //check for special field name
        String docId = doc.get(CassandraUtils.documentIdField);
 
        if(docId == null)
            docId = Long.toHexString(System.nanoTime());
        
       
        ColumnPath termVecColumnPath = new ColumnPath(CassandraUtils.termVecColumnFamily, null, docId.getBytes());
        
        
        int position = 0;

        for (Field field : (List<Field>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term frequencies per doc
                Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
                int lastOffset = 0;
                if (position > 0) {
                    position += analyzer.getPositionIncrementGap(field.name());
                }

                // Build the termPositions vector for all terms
                while (tokens.next(token) != null) {
                    String term = CassandraUtils.createColumnName(field.name(), token.term());
                    allIndexedTerms.add(term);
                    
                    List<Integer> pvec = termPositions.get(term);

                    if (pvec == null) {
                        pvec = new ArrayList<Integer>();
                        termPositions.put(term, pvec);
                    }

                    position += (token.getPositionIncrement() - 1);
                    pvec.add(++position);

                }

                for (Map.Entry<String, List<Integer>> term : termPositions.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all column
                    // families for a key into memory
                    String key = indexName + CassandraUtils.delimeter + term.getKey();

                    CassandraUtils.robustInsert(client, key, termVecColumnPath, CassandraUtils.intVectorToByteArray(term.getValue()));
                }
            }

            //Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized()) {
                String term = CassandraUtils.createColumnName(field.name(), field.stringValue());
                allIndexedTerms.add(term);
                
                String key = indexName + CassandraUtils.delimeter + term;

                CassandraUtils.robustInsert(client, key, termVecColumnPath, CassandraUtils.intVectorToByteArray(Arrays.asList(new Integer[] { 0 })));
            }

            // Stores each field as a column under this doc key
            if (field.isStored()) {

                byte[] value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes();

                ColumnPath docColumnPath = new ColumnPath(CassandraUtils.docColumnFamily, null, field.name().getBytes());

                CassandraUtils.robustInsert(client, indexName+CassandraUtils.delimeter+docId, docColumnPath, value);
            }
        }
        
        //Store meta-data so we can delete this document
        CassandraUtils.robustInsert(client, indexName+CassandraUtils.delimeter+docId, metaColumnPath, CassandraUtils.toBytes(allIndexedTerms));    
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
        try {
            
            ColumnParent cp = new ColumnParent(CassandraUtils.termVecColumnFamily,null);
            List<ColumnOrSuperColumn> docs = client.get_slice(CassandraUtils.keySpace, indexName+CassandraUtils.delimeter+CassandraUtils.createColumnName(term), cp, new SlicePredicate(null,new SliceRange(new byte[]{}, new byte[]{},true,Integer.MAX_VALUE)), ConsistencyLevel.ONE);
                
            //delete by documentId
            for(ColumnOrSuperColumn docInfo : docs){
            
                ColumnOrSuperColumn column = client.get(CassandraUtils.keySpace, indexName+CassandraUtils.delimeter+new String(docInfo.column.name), metaColumnPath, ConsistencyLevel.ONE);
            
                List<String> terms = (List<String>) CassandraUtils.fromBytes(column.column.value);
            
                for(String termStr : terms){
                    ColumnPath termVecColumnPath = new ColumnPath(CassandraUtils.termVecColumnFamily, null, docInfo.column.name);
                    
                    client.remove(CassandraUtils.keySpace, indexName+CassandraUtils.delimeter+termStr, termVecColumnPath, System.currentTimeMillis(), ConsistencyLevel.ONE);
                }
            
                //finally delete ourselves
                client.remove(CassandraUtils.keySpace, indexName+CassandraUtils.delimeter+new String(docInfo.column.name), docAllColumnPath, System.currentTimeMillis(), ConsistencyLevel.ONE);
            }
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (NotFoundException e) {
            return;
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }  
    }

    public int docCount() {

        try {
            String start = indexName + CassandraUtils.delimeter;
            String finish = indexName + CassandraUtils.delimeter + CassandraUtils.delimeter;

            return client.get_key_range(CassandraUtils.keySpace, CassandraUtils.docColumnFamily, start, finish, Integer.MAX_VALUE, ConsistencyLevel.ONE).size();
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }

    }

}
