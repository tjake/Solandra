/**
 * Copyright T Jake Luciani
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
package solandra;

import static org.junit.Assert.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lucandra.CassandraUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolandraTests
{
    static Logger                logger = Logger.getLogger(SolandraTests.class);
    static ExecutorService       eservice;
    static Map<String,CommonsHttpSolrServer> solrClients = new HashMap<String, CommonsHttpSolrServer>();
    static int                   port       = 8983;
    static String                indexName1 = String.valueOf(System.nanoTime());
    static String                indexName2 = String.valueOf(System.nanoTime());
    static String[]              subIndexes = new String[] { "", "one", "two", "three" };

    // Set test schema
    String                       schemaXml  = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
                                                    + "<schema name=\"wikipedia\" version=\"1.1\">\n"
                                                    + "<types>\n"
                                                    + "<fieldType name=\"tint\" class=\"solr.TrieIntField\" precisionStep=\"8\" omitNorms=\"true\" positionIncrementGap=\"0\"/>\n"
                                                    + "<fieldType name=\"text\" class=\"solr.TextField\">\n"
                                                    + "<analyzer><tokenizer class=\"solr.StandardTokenizerFactory\"/></analyzer>\n"
                                                    + "</fieldType>\n"
                                                    + "<fieldType name=\"string\" class=\"solr.StrField\"/>\n"
                                                    + "</types>\n"
                                                    + "<fields>\n"
                                                    + "<field name=\"url\" type=\"string\" indexed=\"true\" stored=\"true\"/>\n"
                                                    + "<field name=\"text\"  type=\"text\" indexed=\"true\"  stored=\"true\" termVectors=\"true\" termPositions=\"true\" termOffsets=\"true\" />\n"
                                                    + "<field name=\"title\" type=\"text\" indexed=\"true\"  stored=\"true\"/>\n"
                                                    + "<field name=\"price\" type=\"tint\" indexed=\"true\"  stored=\"true\"/>\n"
                                                    + "</fields>\n" + "<uniqueKey>url</uniqueKey>\n"
                                                    + "<defaultSearchField>title</defaultSearchField>\n"
                                                    + "</schema>\n";

    @BeforeClass
    public static void setUpBeforeClass()
    {

        CassandraUtils.cacheInvalidationInterval = 0; // real-time
        
        try
        {
            // start cassandra
            CassandraUtils.startup();

            // Start Jetty Solandra Instance
            eservice = Executors.newSingleThreadExecutor();
            eservice.execute(new Runnable() {

                @Override
                public void run()
                {
                    try
                    {
                        JettySolandraRunner jetty = new JettySolandraRunner("/solandra", port);
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

    @Test
    public void allTestsManyIndexes() throws Exception
    {       
        
        assertTrue(!indexName1.equals(indexName2));
        
        for(String core : new String[]{indexName1, indexName2})
        {  
            logger.info("Running tests for core: "+core);
            
            addSchema(core);
            logger.info("added schema");
                        
            for(String vcore : subIndexes  )
            {              
                
                CommonsHttpSolrServer solrClient = getSolrClient(core+"."+vcore);
            
                testOneDocument(solrClient);
                logger.info("testOneDocument");

                testAddData(solrClient);
                logger.info("testAddData");

                testAllSearch(solrClient);
                logger.info("testAllSearch");

                testHighlight(solrClient);
                logger.info("testHighlight");

                testFacets(solrClient);
                logger.info("testFactets");

                testNumericSort(solrClient);
                logger.info("testNumericSort");

                testUnicode(solrClient);
                logger.info("testUnicode");

                testUpdateDocument(solrClient);
                logger.info("testUpdateDocument");
            }
        }
    }
    
    private CommonsHttpSolrServer getSolrClient(String indexName) throws MalformedURLException
    {
        CommonsHttpSolrServer client = solrClients.get(indexName);
        
        if(client == null)
        {
            client = new CommonsHttpSolrServer("http://localhost:" + port + "/solandra/" + indexName);
            solrClients.put(indexName, client);
        }
        
        return client;
    }
    
   

    public void addSchema(final String indexName) throws Exception
    {

        URL url = new URL("http://localhost:" + port + "/solandra/schema/" + indexName);

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

            assertEquals(schemaXml, xml);

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

    
   
    
    public void testOneDocument(CommonsHttpSolrServer solrClient) throws Exception
    {
        
        SolrInputDocument doc = new SolrInputDocument();
        
        doc.addField("title", "test1");
        doc.addField("url", "http://www.test.com");
        doc.addField("text", "this is a test of Solandra");
        doc.addField("price", 1000);

        solrClient.add(doc);
        solrClient.commit(true,true);

        SolrQuery q = new SolrQuery().setQuery("text:Solandra").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(1, r.getResults().getNumFound());
        
        solrClient.deleteById("http://www.test.com");

        solrClient.commit();
        
        r = solrClient.query(q);
        assertEquals(0, r.getResults().getNumFound());
    }

    
    
    
    public void testAddData(CommonsHttpSolrServer solrClient) throws Exception
    {
        
        SolrInputDocument doc = new SolrInputDocument();

        doc.addField("title", "test1");
        doc.addField("url", "http://www.test.com");
        doc.addField("text", "this is a test of Solandra \u5639\u563b");
        doc.addField("price", 1000);

        solrClient.add(doc);

        doc = new SolrInputDocument();

        doc.addField("title", "test2");
        doc.addField("url", "http://www.test2.com");
        doc.addField("text", "this is a test2 of Solandra");
        doc.addField("price", 10000);

        solrClient.add(doc);

        doc = new SolrInputDocument();

        doc.addField("title", "test3");
        doc.addField("url", "http://www.test3.com");
        doc.addField("text", "this is a test3 of Solandra");
        doc.addField("price", 100000);

        solrClient.add(doc);

        doc = new SolrInputDocument();

        doc.addField("title", "test4");
        doc.addField("url", "http://www.test4.com");
        doc.addField("text", "this is a test4 of Solandra");
        doc.addField("price", 10);

        solrClient.add(doc);

        solrClient.commit(true, true);
    }

    
    public void testAllSearch(CommonsHttpSolrServer solrClient) throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("*:*").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(4, r.getResults().getNumFound());
    }

    
    public void testHighlight(CommonsHttpSolrServer solrClient) throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("text:Solandra").addHighlightField("text");

        QueryResponse r = solrClient.query(q);

        SolrDocumentList resultList = r.getResults();

        assertEquals(4, resultList.getNumFound());

        Map<String, Map<String, List<String>>> map = r.getHighlighting();

        assertEquals(1, map.get("http://www.test.com").get("text").size());
    }

    
    public void testFacets(CommonsHttpSolrServer solrClient) throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("text:Solandra").addFacetField("title");

        QueryResponse r = solrClient.query(q);

        SolrDocumentList resultList = r.getResults();

        assertEquals(4, resultList.getNumFound());

        assertEquals(1, r.getFacetFields().size());
    }

    
    public void testNumericSort(CommonsHttpSolrServer solrClient) throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("price:[8 TO 1003]").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(2, r.getResults().getNumFound());
    }

    
    public void testUnicode(CommonsHttpSolrServer solrClient) throws Exception
    {
        SolrQuery q = new SolrQuery().setQuery("text:\u5639\u563b").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);

        assertEquals(1, r.getResults().getNumFound());
    }

    
    public void testDeleteTerm(CommonsHttpSolrServer solrClient) throws Exception
    {

        solrClient.deleteById("http://www.test4.com");
        solrClient.commit(true, true);

        SolrQuery q = new SolrQuery().setQuery("*:*").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(3, r.getResults().getNumFound());

    }

    
    public void testUpdateDocument(CommonsHttpSolrServer solrClient) throws Exception
    {
        SolrInputDocument doc = new SolrInputDocument();

        doc.addField("title", "test1");
        doc.addField("url", "http://www.test.com");
        doc.addField("text", "this is a test of Solandra");
        doc.addField("price", 1000);

        solrClient.add(doc);
        solrClient.commit(true, true);

        SolrQuery q = new SolrQuery().setQuery("text:\u5639\u563b").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(0, r.getResults().getNumFound());
    }

}
