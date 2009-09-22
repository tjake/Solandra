package lucandra;

import junit.framework.TestCase;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

public class LucandraTests extends TestCase {

    public void testWriter() {

        try {
            String indexName = String.valueOf(System.nanoTime());
            Analyzer analyzer = new StandardAnalyzer();
            Cassandra.Client client = CassandraUtils.createConnection();
            IndexWriter indexWriter = new IndexWriter(indexName, client);
            
            Document doc1 = new Document();
            Field f = new Field("key","this is an example value foobar",Field.Store.YES,Field.Index.ANALYZED);
            doc1.add(f);
            
            indexWriter.addDocument(doc1,analyzer);
           
            
            Document doc2 = new Document();
            Field f2 = new Field("key","this is another example",Field.Store.YES,Field.Index.ANALYZED);
            doc2.add(f2);
            indexWriter.addDocument(doc2,analyzer);
            
            
            ColumnParent columnParent = new ColumnParent();
            columnParent.setColumn_family(CassandraUtils.termVecColumn);
            
            assertEquals(4,client.get_count(CassandraUtils.keySpace, indexName, columnParent, ConsistencyLevel.ONE));
            
            
            IndexReader   indexReader = new IndexReader(indexName,client);
            IndexSearcher searcher = new IndexSearcher(indexReader);
            
            
            QueryParser qp = new QueryParser("key",analyzer);
            Query q = qp.parse("+key:another");
            
            TopDocs docs = searcher.search(q,10);
                    
            assertEquals(1,docs.totalHits);
           
            Document doc = searcher.doc(docs.scoreDocs[0].doc);
            
            assertTrue(doc.getField("key") != null);
            
            
            //check something that doesn't exist
            q = qp.parse("+key:bogus");            
            docs = searcher.search(q,10);
                    
            assertEquals(0,docs.totalHits);
           
            
            //check wildcard
            q = qp.parse("+key:anoth*");
            docs = searcher.search(q,10);
            
            assertEquals(1,docs.totalHits);
           
            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

}
