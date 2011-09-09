package solandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lucandra.CassandraUtils;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SolandraTestRunner
{
    static Logger                             logger         = Logger.getLogger(SolandraTests.class);
    static ExecutorService                    eservice;
    static Map<String, CommonsHttpSolrServer> solrClients    = new HashMap<String, CommonsHttpSolrServer>();
    static int                                port           = 8983;
   
    @BeforeClass
    public static void setUpBeforeClass()
    {

        //CassandraUtils.cacheInvalidationInterval = 0; // real-time

        try
        {
            // start cassandra
            CassandraUtils.startupServer();

            // Start Jetty Solandra Instance
            eservice = Executors.newSingleThreadExecutor();
            eservice.execute(new Runnable() {

                @Override
                public void run()
                {
                    try
                    {
                        JettySolandraRunner jetty = new JettySolandraRunner("/solandra", port, "0.0.0.0");
                        jetty.start();
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                    }
                }

            });

            // Wait for ping
            // A raw term query type doesn't check the schema
            URL url = new URL("http://localhost:" + port + "/solandra/select?q={!raw+f=junit_test_query}ping");

            Exception ex = null;
            // Wait for a total of 20 seconds: 100 tries, 200 milliseconds each
            for (int i = 0; i < 20; i++)
            {
                try
                {
                    InputStream stream = url.openStream();
                    stream.close();
                }
                catch (IOException e)
                {
                    // e.printStackTrace();
                    ex = e;
                    Thread.sleep(1000);

                    continue;
                }

            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(0);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        eservice.shutdownNow();
    }

    public static void addSchema(final String indexName, final String schemaXml) throws Exception
    {

        URL url = new URL("http://localhost:" + port + "/solandra/schema/" + indexName + "/schema.xml");

        // write
        try
        {

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(schemaXml);
            wr.flush();
            wr.close();

            assertEquals(200, conn.getResponseCode());

        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail();
        }

        // verify
        try
        {
            InputStream stream = url.openStream();

            BufferedReader rd = new BufferedReader(new InputStreamReader(stream));
            String line;
            String xml = "";
            while ((line = rd.readLine()) != null)
            {
                xml += line + "\n";
            }

            stream.close();

            assertTrue(!xml.isEmpty());

            SolrQuery q = new SolrQuery().setQuery("*:*").addField("*").addField("score");

            QueryResponse r = getSolrClient(indexName).query(q);
            assertEquals(0, r.getResults().getNumFound());

        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail();
        }

    }

    public static CommonsHttpSolrServer getSolrClient(String indexName) throws MalformedURLException
    {
        CommonsHttpSolrServer client = solrClients.get(indexName);

        if (client == null)
        {
            client = new CommonsHttpSolrServer("http://localhost:" + port + "/solandra/" + indexName);
            solrClients.put(indexName, client);
        }

        return client;
    }

    
}
