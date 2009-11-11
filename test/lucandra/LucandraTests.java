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

import java.util.List;

import junit.framework.TestCase;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

public class LucandraTests extends TestCase {

    private static final String indexName = String.valueOf(System.nanoTime());
    private static final Analyzer analyzer = new StandardAnalyzer();
    private static Cassandra.Client client;
    static{
        try{
            client = CassandraUtils.createConnection();
        }catch(Exception e){
            fail(e.getLocalizedMessage());
        }
    }
    
    private static final IndexWriter indexWriter = new IndexWriter(indexName, client);

    public void testWriter() {

        try {

            Document doc1 = new Document();
            Field f = new Field("key", "this is an example value foobar", Field.Store.YES, Field.Index.ANALYZED);
            doc1.add(f);

            indexWriter.addDocument(doc1, analyzer);

            Document doc2 = new Document();
            Field f2 = new Field("key", "this is another example", Field.Store.YES, Field.Index.ANALYZED);
            doc2.add(f2);
            indexWriter.addDocument(doc2, analyzer);

            String start  = indexName+"/";
            String finish = indexName + new Character((char) 255);

            List<String> keys = client.get_key_range(CassandraUtils.keySpace, CassandraUtils.termVecColumnFamily, start, finish, Integer.MAX_VALUE, ConsistencyLevel.ONE);
            assertEquals(4, keys.size());
            assertEquals(2, indexWriter.docCount());
            
            
            //Index 10 documents to test order
            for(int i=300; i>=200; i--){
                Document doc = new Document();
                doc.add(new Field("key", "sort this",Field.Store.YES,Field.Index.ANALYZED));
                doc.add(new Field("date","test"+i,Field.Store.YES, Field.Index.NOT_ANALYZED));
                indexWriter.addDocument(doc, analyzer);
            }
                
            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    public void testReader() {
        try {
            IndexReader indexReader = new IndexReader(indexName, client);
            IndexSearcher searcher = new IndexSearcher(indexReader);

            QueryParser qp = new QueryParser("key", analyzer);
            Query q = qp.parse("+key:another");

            TopDocs docs = searcher.search(q, 10);

            assertEquals(1, docs.totalHits);

            Document doc = searcher.doc(docs.scoreDocs[0].doc);

            assertTrue(doc.getField("key") != null);

            // check something that doesn't exist
            q = qp.parse("+key:bogus");
            docs = searcher.search(q, 10);

            assertEquals(0, docs.totalHits);

            // check wildcard
            q = qp.parse("+key:anoth*");
            docs = searcher.search(q, 10);

            assertEquals(1, docs.totalHits);
            
            Document d = indexReader.document(1);

            String val = new String(d.getBinaryValue("key"),"UTF-8");
            assertTrue(val.equals("this is another example"));
            
            
            // check sort
            Sort sort = new Sort("date");
            q = qp.parse("+key:sort");
            docs = searcher.search(q,null,10, sort);
            
            for(int i=0; i<10; i++){
                d = indexReader.document(docs.scoreDocs[i].doc);
                String dval = new String(d.getBinaryValue("date"));
                assertEquals("test"+(i+200), dval);
            }
            
            
            
         // check wildcard
         q = qp.parse("+date:test*");
         docs = searcher.search(q, 10);

         assertEquals(101, docs.totalHits);
            
            
         // check range queries
         // check wildcard
         q = qp.parse("+key:[apple TO zoo]");
         docs = searcher.search(q, 10);
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }
    
}
