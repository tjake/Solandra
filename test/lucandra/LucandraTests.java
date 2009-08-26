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
            IndexWriter indexWriter = new IndexWriter(indexName, analyzer, client);
            
            Document doc1 = new Document();
            Field f = new Field("key","this is an example value foobar",Field.Store.YES,Field.Index.ANALYZED_NO_NORMS);
            doc1.add(f);
            
            indexWriter.addDocument(doc1);
           
            
            Document doc2 = new Document();
            Field f2 = new Field("key","this is another example",Field.Store.YES,Field.Index.ANALYZED_NO_NORMS);
            doc2.add(f2);
            indexWriter.addDocument(doc2);
            
            ColumnParent columnParent = new ColumnParent();
            columnParent.setColumn_family("Terms");
            
            assertEquals(4,client.get_count(CassandraUtils.keySpace, indexName, columnParent, ConsistencyLevel.ONE));
            
            
            IndexReader   indexReader = new IndexReader(indexName,client);
            IndexSearcher searcher = new IndexSearcher(indexReader);
            
            
            QueryParser qp = new QueryParser("key",analyzer);
            Query q = qp.parse("+key:example");
            
            TopDocs docs = searcher.search(q,10);
            
            assertEquals(2,docs.totalHits);
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

}
