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

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

public class LucandraTests extends TestCase {

    private static final String indexName = String.valueOf(System.nanoTime());
    private static final Analyzer analyzer = new CJKAnalyzer();
    private static Cassandra.Client client;
    static {
        try {
            client = CassandraUtils.createConnection();
        } catch (Exception e) {
            fail(e.getLocalizedMessage());
        }
    }

    private static final IndexWriter indexWriter = new IndexWriter(indexName, client);

    public void testUnicodeSplit(){
        
    }
    
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
            
            
            String start = indexName + CassandraUtils.delimeter;
            String finish = indexName + CassandraUtils.delimeter+CassandraUtils.delimeter;

            ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily);
            SlicePredicate slicePredicate = new SlicePredicate();

            // Get all columns
            SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, true, Integer.MAX_VALUE);
            slicePredicate.setSlice_range(sliceRange);

            List<KeySlice> columns  = client.get_range_slice(CassandraUtils.keySpace, columnParent, slicePredicate, start, finish, 5000, ConsistencyLevel.ONE);
            
            assertEquals(5, columns.size());
            assertEquals(2, indexWriter.docCount());

            // Index 10 documents to test order
            for (int i = 300; i >= 200; i--) {
                Document doc = new Document();
                doc.add(new Field("key", "sort this", Field.Store.YES, Field.Index.ANALYZED));
                doc.add(new Field("date", "test" + i, Field.Store.YES, Field.Index.NOT_ANALYZED));
                indexWriter.addDocument(doc, analyzer);
            }
            
            //Unicode doc
            Document d3 = new Document();          
            d3.add(new Field("key", new String("\u5639\u563b"), Field.Store.YES, Field.Index.ANALYZED));
            indexWriter.addDocument(d3, analyzer);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }
    
    public void testUnicode() throws Exception {
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser("key", analyzer);
        Query q = qp.parse("+key:\u5639\u563b");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document doc = searcher.doc(docs.scoreDocs[0].doc);

        assertNotNull(doc.getField("key"));
    }
    
    public void testDelete() throws Exception {
        indexWriter.deleteDocuments(new Term("key",new String("\u5639\u563b")));
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser("key", analyzer);
        Query q = qp.parse("+key:\u5639\u563b");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(0, docs.totalHits);
    }

    public void testSearch() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser("key", analyzer);
        Query q = qp.parse("+key:another");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document doc = searcher.doc(docs.scoreDocs[0].doc);

        assertNotNull(doc.getField("key"));
    }

    public void testMissingQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser("key", analyzer);

        // check something that doesn't exist
        Query q = qp.parse("+key:bogus");
        TopDocs docs = searcher.search(q, 10);

        assertEquals(0, docs.totalHits);
    }

    public void testWildcardQuery() throws Exception {
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser("key", analyzer);

        // check wildcard
        Query q = qp.parse("+key:anoth*");
        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document d = indexReader.document(1);

        String val = new String(d.getBinaryValue("key"), "UTF-8");
        assertTrue(val.equals("this is another example"));

        // check wildcard
        q = qp.parse("+date:test*");
        docs = searcher.search(q, 10);

        assertEquals(101, docs.totalHits);

    }

    public void testSortQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser("key", analyzer);

        // check sort
        Sort sort = new Sort("date");
        Query q = qp.parse("+key:sort");
        TopDocs docs = searcher.search(q, null, 10, sort);

        for (int i = 0; i < 10; i++) {
            Document d = indexReader.document(docs.scoreDocs[i].doc);
            String dval = new String(d.getBinaryValue("date"));
            assertEquals("test" + (i + 200), dval);
        }

    }

    public void testRangeQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser("key", analyzer);

        // check range queries

        Query q = qp.parse("+key:[apple TO zoo]");
        TopDocs docs = searcher.search(q, 10);
        assertEquals(103, docs.totalHits);

    }

    public void testExactQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser("key", analyzer);

        // check exact
        Query q = qp.parse("+key:\"example value\"");
        TopDocs docs = searcher.search(q, 10);
        assertEquals(1, docs.totalHits);

    }

}
