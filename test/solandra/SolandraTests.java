package solandra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lucandra.CassandraUtils;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolandraCoreContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolandraTests {

    static ExecutorService eservice;
    static CommonsHttpSolrServer solrClient;
    static int port = 8983;
    static String indexName = String.valueOf(System.nanoTime());

    @BeforeClass
    public static void setUpBeforeClass() {

        try {
            // start cassandra
            CassandraUtils.startup();

            // Set test schema
            String schemaXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" 
            + "<schema name=\"wikipedia\" version=\"1.1\">\n" 
            + "<types>\n"
            + "<fieldType name=\"text\" class=\"solr.TextField\">\n"
            + "<analyzer><tokenizer class=\"solr.StandardTokenizerFactory\"/></analyzer>\n"
            + "</fieldType>\n"
            + "<fieldType name=\"string\" class=\"solr.StrField\"/>\n" 
            + "</types>\n"
            + "<fields>\n" 
            + "<field name=\"url\" type=\"string\" indexed=\"true\" stored=\"true\"/>\n"
            + "<field name=\"title\" type=\"text\" indexed=\"true\"  stored=\"true\"/>\n"
            + "<field name=\"text\"  type=\"text\" indexed=\"true\"  stored=\"true\"/>\n" + "</fields>\n" + "<uniqueKey>url</uniqueKey>\n"
            + "<defaultSearchField>title</defaultSearchField>\n" + "</schema>\n";

            SolandraCoreContainer.writeSchema(indexName, schemaXml);

            Thread.sleep(1000);
            
            // Start Jetty Solandra Instance
            eservice = Executors.newSingleThreadExecutor();
            eservice.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        JettySolandraRunner jetty = new JettySolandraRunner("/solr", port);
                        jetty.start();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }

            });

            // Wait for ping
            // A raw term query type doesn't check the schema
            URL url = new URL("http://localhost:" + port + "/solr/" + indexName + "/select?q={!raw+f=junit_test_query}ping");

            Exception ex = null;
            // Wait for a total of 20 seconds: 100 tries, 200 milliseconds each
            for (int i = 0; i < 20; i++) {
                try {
                    InputStream stream = url.openStream();
                    stream.close();
                } catch (IOException e) {
                    // e.printStackTrace();
                    ex = e;
                    Thread.sleep(1000);

                    continue;
                  }

                solrClient = new CommonsHttpSolrServer("http://localhost:" + port + "/solr/" + indexName);
                return;
            }
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(0);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        eservice.shutdownNow();
    }

    @Test
    public void testAddData() throws Exception {

        SolrInputDocument doc = new SolrInputDocument();

        doc.addField("title", "test");
        doc.addField("url", "http://www.test.com");
        doc.addField("text", "this is a test of Solandra");

        solrClient.add(doc); 
        
        
        
    }
    
    @Test
    public void testSearch() throws Exception {
        
        SolrQuery q = new SolrQuery().setQuery("test");
        
        QueryResponse r = solrClient.query(q);
        assertEquals(1,r.getResults().getNumFound());
    }
    

}
