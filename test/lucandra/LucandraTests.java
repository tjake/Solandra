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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.util.Version;

public class LucandraTests extends TestCase {

    private static final String indexName = String.valueOf(System.nanoTime());
    private static final Analyzer analyzer = new CJKAnalyzer(Version.LUCENE_CURRENT);
    private static final String text = "this is an example value foobar foobar";
    private static final String highlightedText = "this is an example value <B>foobar</B> <B>foobar</B>";

    private static Cassandra.Iface client;
    static {
        try {
            client = CassandraUtils.createConnection();
        } catch (Exception e) {
            fail(e.getLocalizedMessage());
        }
    }

    private static final IndexWriter indexWriter = new IndexWriter(indexName, client);

    public void testWriter() throws Exception {

        Document doc1 = new Document();
        Field f = new Field("key", text, Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS);
        doc1.add(f);

        indexWriter.addDocument(doc1, analyzer);

        Document doc2 = new Document();
        Field f2 = new Field("key", "this is another example", Field.Store.YES, Field.Index.ANALYZED);
        doc2.add(f2);
        indexWriter.addDocument(doc2, analyzer);

        String start = CassandraUtils.hashKey(indexName + CassandraUtils.delimeter + "key" + CassandraUtils.delimeter);
        String finish = "";
        
        ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily);
        SlicePredicate slicePredicate = new SlicePredicate();

        // Get all columns
        SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, true, Integer.MAX_VALUE);
        slicePredicate.setSlice_range(sliceRange);

        List<KeySlice> columns = client.get_range_slice(CassandraUtils.keySpace, columnParent, slicePredicate, start, finish, 5000, ConsistencyLevel.ONE);

        int matchingColumns = 0;
        for(KeySlice ks : columns){
            String termStr = ks.getKey().substring(ks.getKey().indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());
            Term term = CassandraUtils.parseTerm(termStr);
            
            if(term.field().equals("key") && ks.getKey().equals(CassandraUtils.hashKey(indexName+CassandraUtils.delimeter+term.field()+CassandraUtils.delimeter+term.text())))
                matchingColumns++;
              
        }

        // Index 10 documents to test order
        for (int i = 300; i >= 200; i--) {
            Document doc = new Document();
            doc.add(new Field("key", "sort this", Field.Store.YES, Field.Index.ANALYZED));
            doc.add(new Field("date", "test" + i, Field.Store.YES, Field.Index.NOT_ANALYZED));
            indexWriter.addDocument(doc, analyzer);
        }

        // Unicode doc
        Document d3 = new Document();
        d3.add(new Field("key", new String("\u5639\u563b"), Field.Store.YES, Field.Index.ANALYZED));
        d3.add(new Field("key", new String("samefield"), Field.Store.YES, Field.Index.ANALYZED));
        d3.add(new Field("url", "http://www.google.com", Field.Store.YES, Field.Index.NOT_ANALYZED));
        indexWriter.addDocument(d3, analyzer);

        
        //
        assertEquals(5, matchingColumns);
        assertEquals(104, indexWriter.docCount());
        
    }

    public void testUnicode() throws Exception {
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);
        Query q = qp.parse("+key:\u5639\u563b");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document doc = searcher.doc(docs.scoreDocs[0].doc);

        assertNotNull(doc.getField("key"));
    }

    public void testMultiValuedFields() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);
        Query q = qp.parse("+key:samefield");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document doc = searcher.doc(docs.scoreDocs[0].doc);

        String field = doc.getField("key").stringValue();
        String cmp   = "\u5639\u563b samefield";
        assertEquals(field,cmp);
    }

    public void testKeywordField() throws Exception {
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        
        TermQuery tq = new TermQuery(new Term("url", "http://www.google.com"));
        TopDocs topDocs = searcher.search(tq, 10);
        
        assertEquals(topDocs.totalHits,1);
        
    }
    
    public void testDelete() throws Exception {
        indexWriter.deleteDocuments(new Term("key", new String("\u5639\u563b")));
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);
        Query q = qp.parse("+key:\u5639\u563b");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(0, docs.totalHits);
    }

    public void testSearch() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);
        Query q = qp.parse("+key:another");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document doc = searcher.doc(docs.scoreDocs[0].doc);

        assertNotNull(doc.getField("key"));
    }

    public void testScore() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);
        Query q = qp.parse("+key:example");

        TopDocs docs = searcher.search(q, 10);

        assertEquals(2, docs.totalHits);

        Document doc = searcher.doc(docs.scoreDocs[0].doc);

        String fld = doc.getField("key").stringValue();
        // Highest scoring doc should be the one with higher boost
        assertEquals(fld, "this is another example");

    }

    public void testMissingQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);

        // check something that doesn't exist
        Query q = qp.parse("+key:bogus");
        TopDocs docs = searcher.search(q, 10);

        assertEquals(0, docs.totalHits);
    }

    public void testWildcardQuery() throws Exception {
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);

        // check wildcard
        Query q = qp.parse("+key:anoth*");
        TopDocs docs = searcher.search(q, 10);

        assertEquals(1, docs.totalHits);

        Document d = indexReader.document(1);

        String val = d.get("key");
        assertTrue(val.equals("this is another example"));

        // check wildcard
        q = qp.parse("+date:test*");
        docs = searcher.search(q, 10);

        assertEquals(101, docs.totalHits);

    }

    public void testSortQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);

        // check sort
        Sort sort = new Sort(new SortField("date", SortField.STRING));
        Query q = qp.parse("+key:sort");
        TopDocs docs = searcher.search(q, null, 10, sort);

        for (int i = 0; i < 10; i++) {
            Document d = indexReader.document(docs.scoreDocs[i].doc);
            String dval = d.get("date");
            assertEquals("test" + (i + 200), dval);
        }

    }

    public void testRangeQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);

        // check range queries

        Query q = qp.parse("+key:[apple TO zoo]");
        TopDocs docs = searcher.search(q, 10);
        assertEquals(103, docs.totalHits);

    }

    public void testExactQuery() throws Exception {

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);

        // check exact
        Query q = qp.parse("+key:\"foobar foobar\"");
        TopDocs docs = searcher.search(q, 10);
        assertEquals(1, docs.totalHits);

        q = qp.parse("+key:\"not in index\"");
        docs = searcher.search(q, 10);
        assertEquals(0, docs.totalHits);
        
         q = qp.parse("+key:\"is an\"");
         docs = searcher.search(q, 10);
        assertEquals(1, docs.totalHits);
        
    }

    public void testSimpleAnalyzerWriteRead() throws Exception {
        
        Document doc = new Document();
        Field f = new Field("title", text, Field.Store.YES, Field.Index.ANALYZED);
        doc.add(f);
        indexWriter.addDocument(doc, analyzer);
        
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "title", analyzer);
        
        Query q = qp.parse("foobar");
        TopDocs docs = searcher.search(q, 10);
        assertEquals(1, docs.totalHits);
        
        q = qp.parse("\"not in index\"");
        docs = searcher.search(q, 10);
        assertEquals(0, docs.totalHits);
        
        indexReader.reopen();
        
        q = qp.parse("\"foobar foobar\"");
        docs = searcher.search(q, 10);
        assertEquals(0, docs.totalHits);
    }
    
    public void testHighlight() throws Exception {

        // This tests the TermPositionVector classes

        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);

        // check exact
        Query q = qp.parse("+key:\"foobar foobar\"");
        TopDocs docs = searcher.search(q, 10);
        assertEquals(1, docs.totalHits);

        SimpleHTMLFormatter formatter = new SimpleHTMLFormatter();
        QueryScorer scorer = new QueryScorer(q, "key", text);
        Highlighter highlighter = new Highlighter(formatter, scorer);
        highlighter.setTextFragmenter(new SimpleFragmenter(Integer.MAX_VALUE));

        TokenStream tvStream = TokenSources.getTokenStream(indexReader, docs.scoreDocs[0].doc, "key");

        String rv = highlighter.getBestFragment(tvStream, text);

        assertNotNull(rv);
        assertEquals(rv, highlightedText);
    }
    
    public void testLucandraFilter() throws Exception {
        IndexReader indexReader = new IndexReader(indexName, client);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        try {
            for (int i = 0; i < 10; i++) {
                Document doc1 = new Document();
                doc1.add(new Field("aKey", "aKey"+i, Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
                doc1.add(new Field("category", "category1", Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
                indexWriter.addDocument(doc1, analyzer);
            }

            
            QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "aKey", analyzer);
            Query q = qp.parse("aKey1");
            LucandraFilter filter =  new LucandraFilter();
            filter.addTerm(new Term("category", "category1"));
            TopDocs docs = searcher.search(q,filter, 10);
            assertEquals(1, docs.totalHits);

            indexReader.reopen();
            
            q = qp.parse("aKey1 OR aKey2");

            docs = searcher.search(q,filter, 10);
            assertEquals(2, docs.totalHits);

            indexReader.reopen();

            
            q = qp.parse("[aKey0 TO aKey5]");

            docs = searcher.search(q,filter, 10);
            assertEquals(6, docs.totalHits);

            indexReader.reopen();

            
            filter =  new LucandraFilter();
            filter.addTerm(new Term("category", "category0"));

            docs = searcher.search(q,filter, 10);
            assertEquals(0, docs.totalHits);

        } catch (Exception e) {
             e.printStackTrace();
              fail(e.toString());
        }

    }
}
