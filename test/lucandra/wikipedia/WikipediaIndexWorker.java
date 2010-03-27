package lucandra.wikipedia;

import java.util.concurrent.Callable;

import lucandra.CassandraUtils;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.thrift.transport.TTransportException;


public class WikipediaIndexWorker implements Callable<Boolean>{

    //each worker thread has a connection to cassandra
    private static ThreadLocal<lucandra.IndexWriter> clientPool = new ThreadLocal<lucandra.IndexWriter>();
    
    //this is shared by all workers
    private static Analyzer   analyzer = new SimpleAnalyzer();
    
    //this is the article to index
    private Article   article;  
    
    public WikipediaIndexWorker(Article article){
       
        this.article = article;
    }
    
    private lucandra.IndexWriter getIndexWriter() throws TTransportException{
        lucandra.IndexWriter indexWriter = clientPool.get();
        
       
        if(indexWriter == null){
            indexWriter = new lucandra.IndexWriter("wikipedia",CassandraUtils.createConnection());
            clientPool.set(indexWriter);
        }
        
        return indexWriter;      
    }
    
    public Boolean call() throws Exception {
        
        lucandra.IndexWriter indexWriter = getIndexWriter();
        
        Document d = new Document();
        
        d.add(new Field("title",article.title,Store.YES, Index.ANALYZED));
        
        if(article.text != null)
            d.add(new Field("text", new String(article.text),Store.YES, Index.ANALYZED));
        
        d.add(new Field("url",article.url,Store.YES, Index.NOT_ANALYZED));
        
        indexWriter.addDocument(d, analyzer);
        
        return true;
    }

}
