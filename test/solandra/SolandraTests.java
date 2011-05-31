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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lucandra.CassandraUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolandraTests
{
    static Logger                             logger         = Logger.getLogger(SolandraTests.class);
    static ExecutorService                    eservice;
    static Map<String, CommonsHttpSolrServer> solrClients    = new HashMap<String, CommonsHttpSolrServer>();
    static int                                port           = 8983;
    static String                             indexName1     = String.valueOf(System.nanoTime());
    static String                             indexName2     = String.valueOf(System.nanoTime());
    static String[]                           subIndexes     = new String[] { "", "one", "two", "three" };

    static String                             otherIndexName = String.valueOf(System.nanoTime());
    static CommonsHttpSolrServer              otherClient;

    // Set test schema
    String                                    schemaXml      = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
                                                                     + "<schema name=\"wikipedia\" version=\"1.1\">\n"
                                                                     + "<types>\n"
                                                                     + "<fieldType name=\"tint\" class=\"solr.TrieIntField\" precisionStep=\"8\" omitNorms=\"true\" positionIncrementGap=\"0\"/>\n"
                                                                     + "<fieldType name=\"text\" class=\"solr.TextField\">\n"
                                                                     + "<analyzer><tokenizer class=\"solr.StandardTokenizerFactory\"/></analyzer>\n"
                                                                     + "</fieldType>\n"
                                                                     + "<fieldType name=\"string\" class=\"solr.StrField\"/>\n"
                                                                     + "<fieldType name=\"sint\" class=\"solr.SortableIntField\" omitNorms=\"true\"/>\n"
                                                                     + "</types>\n"
                                                                     + "<fields>\n"
                                                                     + "<field name=\"url\" type=\"string\" indexed=\"true\" stored=\"true\"/>\n"
                                                                     + "<field name=\"text\"  type=\"text\" indexed=\"true\"  stored=\"true\" termVectors=\"true\" termPositions=\"true\" termOffsets=\"true\"/>\n"
                                                                     + "<field name=\"title\" type=\"text\" indexed=\"true\"  stored=\"true\"/>\n"
                                                                     + "<field name=\"price\" type=\"tint\" indexed=\"true\"  stored=\"true\"/>\n"
                                                                     + "<dynamicField name=\"*_i\" stored=\"false\" type=\"sint\" multiValued=\"false\" indexed=\"true\"/>"
                                                                     + "</fields>\n"
                                                                     + "<uniqueKey>url</uniqueKey>\n"
                                                                     + "<defaultSearchField>title</defaultSearchField>\n"
                                                                     + "</schema>\n";

    @BeforeClass
    public static void setUpBeforeClass()
    {

        CassandraUtils.cacheInvalidationInterval = 0; // real-time

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

        for (String core : new String[] { indexName1, indexName2 })
        {
            logger.info("Running tests for core: " + core);

            addSchema(core);
            logger.info("added schema");

            for (String vcore : subIndexes)
            {

                CommonsHttpSolrServer solrClient = getSolrClient(core + "." + vcore);

                testOneDocument(solrClient);
                logger.info("testOneDocument");

                testAddData(solrClient);
                logger.info("testAddData");

                testPhraseSearch(solrClient);
                logger.info("testPhraseSearch");
                
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

                testDeleteTerm(solrClient);
                logger.info("testDeleteTerm");
                
                testUpdateDocument(solrClient);
                logger.info("testUpdateDocument");
                
                testWildcardSearch(solrClient);
                logger.info("testWildCardSearch");
                                             
                testQueryFilter(solrClient);
                logger.info("testQueryFilter");
                
                testDeleteByQuery(solrClient);
                logger.info("testDeleteByQuery");
                
            
            }
        }
    }

    private CommonsHttpSolrServer getSolrClient(String indexName) throws MalformedURLException
    {
        CommonsHttpSolrServer client = solrClients.get(indexName);

        if (client == null)
        {
            client = new CommonsHttpSolrServer("http://localhost:" + port + "/solandra/" + indexName);
            solrClients.put(indexName, client);
        }

        return client;
    }

    public void addSchema(final String indexName) throws Exception
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

    public void testOneDocument(CommonsHttpSolrServer solrClient) throws Exception
    {

        SolrInputDocument doc = new SolrInputDocument();

        doc.addField("title", "test1");
        doc.addField("url", "http://www.test.com");
        doc.addField("text", "this is a test of Solandra");
        doc.addField("price", 1000);

        solrClient.add(doc);
        solrClient.commit(true, true);

        SolrQuery q = new SolrQuery().setQuery("text:Solandra").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        
        assertEquals(1, r.getResults().getNumFound());

        solrClient.deleteById("http://www.test.com");

        solrClient.commit(true, true);

        r = solrClient.query(q);
        assertEquals(0, r.getResults().getNumFound());
    }

    public void testAddData(CommonsHttpSolrServer solrClient) throws Exception
    {

        SolrInputDocument doc = new SolrInputDocument();

        doc.addField("title", "test1");
        doc.addField("url", "http://www.test.com");
        doc.addField("text", "this is a test of Solandra \u5639\u563b");
        doc.addField("user_id_i", 10);
        doc.addField("price", 1000);

        solrClient.add(doc);

        doc = new SolrInputDocument();

        doc.addField("title", "test2");
        doc.addField("url", "http://www.test2.com");
        doc.addField("text", "this is a test2 of Solandra");
        doc.addField("user_id_i", 10);
        doc.addField("price", 10000);

        solrClient.add(doc);

        doc = new SolrInputDocument();

        doc.addField("title", "test3");
        doc.addField("url", "http://www.test3.com");
        doc.addField("text", "this is a test3 of Solandra");
        doc.addField("user_id_i", 100);
        doc.addField("price", 100000);

        solrClient.add(doc);

        doc = new SolrInputDocument();

        doc.addField("title", "test4");
        doc.addField("url", "http://www.test4.com");
        doc.addField("text", "this is a test4 of Solandra");
        doc.addField("user_id_i", 100);
        doc.addField("price", 10);

        solrClient.add(doc);

        solrClient.commit(true, true);
    }

    public void testPhraseSearch(CommonsHttpSolrServer solrClient) throws Exception
    {
        SolrQuery q = new SolrQuery().setQuery("text:\"test4 of Solandra\"").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(1, r.getResults().getNumFound());
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
        doc.addField("user_id_i", 10);
        doc.addField("price", 1000);

        solrClient.add(doc);
        solrClient.commit(true, true);

        SolrQuery q = new SolrQuery().setQuery("text:\u5639\u563b").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(0, r.getResults().getNumFound());
    }

    public void testWildcardSearch(CommonsHttpSolrServer solrClient) throws Exception
    {
        SolrQuery q = new SolrQuery().setQuery("url:[* TO *]").addField("*").addField("score");

        QueryResponse r = solrClient.query(q);
        assertEquals(3, r.getResults().getNumFound());
    }

    public void testDeleteByQuery(CommonsHttpSolrServer solrClient) throws Exception
    {
        solrClient.deleteByQuery("*:*");
        solrClient.commit(true, true);

        SolrQuery q = new SolrQuery().setQuery("*:*").addField("*").addField("score");
        QueryResponse r = solrClient.query(q);

        assertEquals(0, r.getResults().getNumFound());
    }
    
    public void testQueryFilter(CommonsHttpSolrServer solrClient) throws Exception
    {
        SolrQuery q = new SolrQuery().setQuery("text:Solandra").addFilterQuery("user_id_i:10");

        QueryResponse r = solrClient.query(q);

        assertEquals(2, r.getResults().getNumFound());
    }
    

    @Test
    public void setAddOtherSchema() throws Exception
    {

        otherClient = new CommonsHttpSolrServer("http://localhost:" + port + "/solandra/" + otherIndexName );

        String otherSchema = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
                + "<schema name=\"config\" version=\"1.2\">\n"
                + "<types>\n"
                + "<fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" omitNorms=\"true\"/>\n"
                + "<fieldType name=\"long\" class=\"solr.TrieLongField\" precisionStep=\"0\" omitNorms=\"true\" positionIncrementGap=\"0\"/>\n"
                + "<fieldType name=\"text_ws\" class=\"solr.TextField\" positionIncrementGap=\"100\">\n"
                + "<analyzer><tokenizer class=\"solr.WhitespaceTokenizerFactory\"/></analyzer>\n"
                + "</fieldType>\n"
                + "</types>\n"
                + "<fields>\n"
                + "<field name=\"messageType\" type=\"text_ws\" indexed=\"true\" stored=\"true\" required=\"true\" />\n"
                + "<field name=\"modTime\" type=\"long\" indexed=\"true\" stored=\"false\" required=\"false\" />\n"
                + "<field name=\"uuid\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\" />\n"
                + "<field name=\"ownerUUID\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"false\" />\n"
                + "<field name=\"generatorUUID\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"false\" />\n"
                + "<field name=\"key\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" />\n"
                + "<field name=\"name\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"false\" />\n"
                + "<field name=\"desc\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" />\n"
                + "<field name=\"defaultValue\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" />\n"
                + "<field name=\"value\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" />\n"
                + "<field name=\"attributeDef\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" multiValued=\"true\" />\n"
                + "<field name=\"instructionDef\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" multiValued=\"true\" />\n"
                + "<field name=\"attribute\" type=\"string\" indexed=\"true\" stored=\"false\" required=\"false\" multiValued=\"true\" />\n"
                + "<field name=\"json\" type=\"text_ws\" indexed=\"false\" stored=\"true\" />\n" + "</fields>\n"
                + "<uniqueKey>uuid</uniqueKey>\n" + "<defaultSearchField>uuid</defaultSearchField>\n" + "</schema>\n";

        URL url = new URL("http://localhost:" + port + "/solandra/schema/" + otherIndexName + "/schema.xml");

        // write
        try
        {

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(otherSchema);
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

            //
            try
            {
                QueryResponse r = otherClient.query(q);
                assertEquals(0, r.getResults().getNumFound());
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }

        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testOneOtherDocument() throws Exception
    {
        String doc = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">restart</field><field name=\"name\">Stop and Restart the Resource Manager</field><field name=\"desc\">Stop and Restart the Resource Manager</field><field name=\"uuid\">4a15da95-f8aa-4039-ba49-f057d23004fa</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"4a15da95-f8aa-4039-ba49-f057d23004fa\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"restart\",\"name\":\"Stop and Restart the Resource Manager\",\"desc\":\"Stop and Restart the Resource Manager\"}}</field></doc></add>";

        URL url = new URL("http://localhost:" + port + "/solandra/" + otherIndexName + "/update?commit=true");

        // write
        try
        {

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-Type", "text/xml");
            conn.setDoOutput(true);

            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(doc);
            wr.flush();
            wr.close();

            assertEquals(200, conn.getResponseCode());

        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail();
        }

        SolrQuery q = new SolrQuery().setQuery("uuid:4a15da95-f8aa-4039-ba49-f057d23004fa");

        QueryResponse r = otherClient.query(q);
        assertEquals(1, r.getResults().getNumFound());
    }

    @Test
    public void testAllUUIDSearch() throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("uuid:[* TO *]");

        QueryResponse r = otherClient.query(q);
        assertEquals(1, r.getResults().getNumFound());
    }

    @Test
    public void testSomeOtherDocuments() throws Exception
    {
        Collection<String> docs = new ArrayList<String>();
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp7</field><field name=\"name\">Generated datapoint 7</field><field name=\"uuid\">51c43d10-668a-4a7c-a92b-e38230a65827</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"someAttr\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"someAttr\",\"name\":\"Just a random attribute\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"51c43d10-668a-4a7c-a92b-e38230a65827\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp7\",\"name\":\"Generated datapoint 7\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">Analog 1</field><field name=\"name\">Voltage</field><field name=\"uuid\">0bbfe967-8326-45a2-a2ea-eafe366cc56d</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"Units\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Units\",\"name\":\"Units\",\"desc\":\"\",\"defaultValue\":\"V\"}},\"isMap\":false,\"uuid\":\"0bbfe967-8326-45a2-a2ea-eafe366cc56d\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Analog 1\",\"name\":\"Voltage\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp6</field><field name=\"name\">Generated datapoint 6</field><field name=\"uuid\">1fbed205-8dcd-4eb3-8562-3bd55fb01759</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"someAttr\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"someAttr\",\"name\":\"Just a random attribute\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"1fbed205-8dcd-4eb3-8562-3bd55fb01759\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp6\",\"name\":\"Generated datapoint 6\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp9</field><field name=\"name\">Generated datapoint 9</field><field name=\"uuid\">889c7b35-a5c0-4692-b9a1-5eacc33c9c46</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"someAttr\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"someAttr\",\"name\":\"Just a random attribute\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"889c7b35-a5c0-4692-b9a1-5eacc33c9c46\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp9\",\"name\":\"Generated datapoint 9\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp8</field><field name=\"name\">Generated datapoint 8</field><field name=\"uuid\">753fdc94-04f4-4be6-abf4-028363acddfb</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"someAttr\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"someAttr\",\"name\":\"Just a random attribute\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"753fdc94-04f4-4be6-abf4-028363acddfb\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp8\",\"name\":\"Generated datapoint 8\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">commAlarm</field><field name=\"name\">Comm Alarm</field><field name=\"uuid\">3c4752c1-9fc3-4af1-afaf-749307adb419</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"sevLevel\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"3c4752c1-9fc3-4af1-afaf-749307adb419\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"commAlarm\",\"name\":\"Comm Alarm\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">Base Field 999</field><field name=\"name\">Base Field 999</field><field name=\"uuid\">5584c6ee-be2f-4ce4-95b0-227638e971ad</field><field name=\"json\">{\"DatapointDef\":{\"isMap\":false,\"uuid\":\"5584c6ee-be2f-4ce4-95b0-227638e971ad\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Base Field 999\",\"name\":\"Base Field 999\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">stop</field><field name=\"name\">Overridden stop instruction</field><field name=\"desc\">Overridden stop instruction</field><field name=\"uuid\">65edf22a-0290-4517-9de5-9ea198093a13</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"65edf22a-0290-4517-9de5-9ea198093a13\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"stop\",\"name\":\"Overridden stop instruction\",\"desc\":\"Overridden stop instruction\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">start</field><field name=\"name\">Default start instruction.</field><field name=\"desc\">Default start instruction.</field><field name=\"uuid\">63fcbe39-dc69-4279-b942-e3ff2630cf55</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"63fcbe39-dc69-4279-b942-e3ff2630cf55\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"start\",\"name\":\"Default start instruction.\",\"desc\":\"Default start instruction.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">setPollRate2</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">652e7f6f-00a8-4fc5-a187-7a5fe923ef0e</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"652e7f6f-00a8-4fc5-a187-7a5fe923ef0e\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"setPollRate2\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">setPollRate</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">d3cebc10-ed42-4ee6-9b15-523b4f2c0109</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"d3cebc10-ed42-4ee6-9b15-523b4f2c0109\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"setPollRate\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">initialize</field><field name=\"name\">Inititialize the resource with the following parameters.</field><field name=\"desc\">Inititialize the resource with the following parameters.</field><field name=\"uuid\">23685857-3daa-41ea-bdbe-8d0a69ab2742</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":20,\"key\":\"A\",\"name\":\"Position Name\",\"desc\":\"Position Name\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.01,\"precision\":2,\"units\":\"seconds\",\"key\":\"B\",\"name\":\"UTC Time\",\"desc\":\"UTC Time\",\"type\":\"Number\"},{\"listEntries\":[\"Clockwise\",\"Counterclockwise\",\"Shortest Path\"],\"selectedEntries\":[],\"key\":\"C\",\"name\":\"Cable Wrap\",\"desc\":\"Cable Wrap\",\"type\":\"List\"},{\"key\":\"D\",\"name\":\"Some File Parm\",\"desc\":\"Desc for some file parm\",\"type\":\"File\"},{\"options\":[{\"isSelected\":false,\"key\":\"Pick Me\",\"name\":\"Pick Me\",\"desc\":\"Pick Me\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"},\"key\":\"E\",\"name\":\"A group bool parm\",\"desc\":\"A group bool parm\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"23685857-3daa-41ea-bdbe-8d0a69ab2742\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"initialize\",\"name\":\"Inititialize the resource with the following parameters.\",\"desc\":\"Inititialize the resource with the following parameters.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Resource</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"name\">TCP Resource 1</field><field name=\"desc\">This is a TCP resource</field><field name=\"uuid\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"json\">{\"Resource\":{\"uuid\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"name\":\"TCP Resource 1\",\"desc\":\"This is a TCP resource\",\"transportUUID\":\"9186e0bc-d862-45d5-a461-a09aa52034ca\",\"translatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">I'm a new datapoint</field><field name=\"name\">Error Rate</field><field name=\"uuid\">a36bf16e-b94e-427d-b78e-eb37f2263720</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"Units\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Units\",\"name\":\"Units\",\"desc\":\"\",\"defaultValue\":\"Err/sec\"}},\"isMap\":false,\"uuid\":\"a36bf16e-b94e-427d-b78e-eb37f2263720\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"I\u0027m a new datapoint\",\"name\":\"Error Rate\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">Base Fault 1</field><field name=\"name\">Fault 1</field><field name=\"desc\">Fault 1 Description</field><field name=\"uuid\">f04d7224-3190-4379-84e8-53d7034d8a4f</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"sevLevel\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\"},\"helpText\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"helpText\",\"name\":\"helpText\",\"desc\":\"If this fault occurs...run for the hills!!\"},\"units\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"units\",\"name\":\"Units\",\"desc\":\"Units\"}},\"isMap\":false,\"uuid\":\"f04d7224-3190-4379-84e8-53d7034d8a4f\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Base Fault 1\",\"name\":\"Fault 1\",\"desc\":\"Fault 1 Description\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">some key</field><field name=\"name\">My new datapoint</field><field name=\"uuid\">08bbf4b6-70b8-4da4-a3af-f75ed30a246d</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"sevLevel\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\",\"defaultValue\":\"0\"},\"resName\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"resName\",\"name\":\"Resource Name\",\"desc\":\"\"}},\"instructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":10,\"key\":\"some key\",\"name\":\"My new datapoint\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":255.0,\"stepSize\":0.0,\"precision\":0,\"key\":\"sevLevel\",\"name\":\"severity level\",\"type\":\"Number\"},{\"minLength\":0,\"maxLength\":10,\"key\":\"resName\",\"name\":\"Resource Name\",\"type\":\"String\"}],\"key\":\"some key\",\"name\":\"My new datapoint\",\"desc\":\"\"},\"isMap\":false,\"uuid\":\"08bbf4b6-70b8-4da4-a3af-f75ed30a246d\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"some key\",\"name\":\"My new datapoint\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">restart</field><field name=\"name\">Stop and Restart the Resource Manager</field><field name=\"desc\">Stop and Restart the Resource Manager</field><field name=\"uuid\">4a15da95-f8aa-4039-ba49-f057d23004fa</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"4a15da95-f8aa-4039-ba49-f057d23004fa\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"restart\",\"name\":\"Stop and Restart the Resource Manager\",\"desc\":\"Stop and Restart the Resource Manager\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">updateSharedModule</field><field name=\"name\">Add/Update a shared translator module</field><field name=\"desc\">Add/Update a shared translator module</field><field name=\"uuid\">90a92cd3-3e95-47d6-a3b0-a86c2f298eac</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"uuid\",\"name\":\"Shared Module ID\",\"desc\":\"Shared Module ID\",\"type\":\"String\"}],\"uuid\":\"90a92cd3-3e95-47d6-a3b0-a86c2f298eac\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"updateSharedModule\",\"name\":\"Add/Update a shared translator module\",\"desc\":\"Add/Update a shared translator module\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">delTranslator</field><field name=\"name\">Delete a translator</field><field name=\"desc\">Delete a translator</field><field name=\"uuid\">90a817a7-6812-4cfc-a4dd-a78718971b1b</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"translatorUUID\",\"name\":\"Translator ID\",\"desc\":\"Translator ID\",\"type\":\"String\"}],\"uuid\":\"90a817a7-6812-4cfc-a4dd-a78718971b1b\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"delTranslator\",\"name\":\"Delete a translator\",\"desc\":\"Delete a translator\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">TestInstruction</field><field name=\"name\">This is a test instruction</field><field name=\"desc\">This is a test instruction</field><field name=\"uuid\">5c87542d-bcf8-401e-b5ff-ec4b324a7ddf</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":20,\"key\":\"A\",\"name\":\"Position Name\",\"desc\":\"Position Name\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.01,\"precision\":2,\"units\":\"seconds\",\"key\":\"B\",\"name\":\"UTC Time\",\"desc\":\"UTC Time\",\"type\":\"Number\"},{\"listEntries\":[\"Clockwise\",\"Counterclockwise\",\"Shortest Path\"],\"selectedEntries\":[],\"key\":\"C\",\"name\":\"Cable Wrap\",\"desc\":\"Cable Wrap\",\"type\":\"List\"},{\"key\":\"D\",\"name\":\"Some File Parm\",\"desc\":\"Desc for some file parm\",\"type\":\"File\"},{\"options\":[{\"isSelected\":false,\"key\":\"Pick Me\",\"name\":\"Pick Me\",\"desc\":\"Sometimes pick this option\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"and other times pick this one.\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"and other times pick this one.\",\"type\":\"SimpleBool\"},\"key\":\"E\",\"name\":\"A group bool parm\",\"desc\":\"A group bool parm\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"5c87542d-bcf8-401e-b5ff-ec4b324a7ddf\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"TestInstruction\",\"name\":\"This is a test instruction\",\"desc\":\"This is a test instruction\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">Analog 2</field><field name=\"name\">Error Rate</field><field name=\"uuid\">9e834293-ae7f-4b79-8631-5daa40c1cee1</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"Units\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Units\",\"name\":\"Units\",\"desc\":\"\",\"defaultValue\":\"Err/sec\"}},\"isMap\":false,\"uuid\":\"9e834293-ae7f-4b79-8631-5daa40c1cee1\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Analog 2\",\"name\":\"Error Rate\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp5</field><field name=\"name\">Generated datapoint 5</field><field name=\"uuid\">a4b29317-5c85-4b15-a57f-4c816235f70f</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"someAttr\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"someAttr\",\"name\":\"Just a random attribute\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"a4b29317-5c85-4b15-a57f-4c816235f70f\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp5\",\"name\":\"Generated datapoint 5\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp4</field><field name=\"name\">Generated datapoint 4</field><field name=\"uuid\">7028ad70-5d5d-435d-9573-39078b12ff69</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"someAttr\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"someAttr\",\"name\":\"Just a random attribute\",\"desc\":\"\"}},\"isMap\":false,\"uuid\":\"7028ad70-5d5d-435d-9573-39078b12ff69\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp4\",\"name\":\"Generated datapoint 4\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">Analog 0</field><field name=\"name\">Temperature</field><field name=\"uuid\">37c533aa-fd2a-4f43-af28-e4970262053b</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"Units\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Units\",\"name\":\"Units\",\"desc\":\"\",\"defaultValue\":\"F\"},\"sevLevel\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\",\"defaultValue\":\"0\"}},\"isMap\":false,\"uuid\":\"37c533aa-fd2a-4f43-af28-e4970262053b\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"Analog 0\",\"name\":\"Temperature\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Transport</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440002</field><field name=\"name\">TCP Socket B</field><field name=\"desc\">New Transport's description</field><field name=\"uuid\">9186e0bc-d862-45d5-a461-a09aa52034ca</field><field name=\"json\">{\"Transport\":{\"uuid\":\"9186e0bc-d862-45d5-a461-a09aa52034ca\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440002\",\"name\":\"TCP Socket B\",\"desc\":\"New Transport\u0027s description\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Transport</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440002</field><field name=\"name\">TCP Socket A</field><field name=\"desc\">New Transport's description</field><field name=\"uuid\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"json\">{\"Transport\":{\"uuid\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440002\",\"name\":\"TCP Socket A\",\"desc\":\"New Transport\u0027s description\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Transport</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">458724cf-202e-49c3-9863-0e9c176cc27a</field><field name=\"name\">SNMP Manager</field><field name=\"desc\">This is an SNMP Manager transport</field><field name=\"uuid\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"json\">{\"Transport\":{\"uuid\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"458724cf-202e-49c3-9863-0e9c176cc27a\",\"name\":\"SNMP Manager\",\"desc\":\"This is an SNMP Manager transport\"}}</field></doc></add>");

        URL url = new URL("http://localhost:" + port + "/solandra/" + otherIndexName + "/update?commit=true");

        writeDocs(docs, url);

        SolrQuery q = new SolrQuery().setQuery("*:*").addField("*").addField("score");

        try
        {
            QueryResponse r = otherClient.query(q);
            assertEquals(27, r.getResults().getNumFound());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Test
    public void testAllUUIDSearchAgain() throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("uuid:[* TO *]");

        QueryResponse r = otherClient.query(q);
        assertEquals(27, r.getResults().getNumFound());
    }

    @Test
    public void testAllMessageTypeSearch() throws Exception
    {
        SolrQuery q = new SolrQuery().setQuery("messageType:[* TO *]");

        QueryResponse r = otherClient.query(q);
        assertEquals(27, r.getResults().getNumFound());
    }

    @Test
    public void testAllMessageTypeTransportSearch() throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("messageType:Transport");

        QueryResponse r = otherClient.query(q);
        assertEquals(3, r.getResults().getNumFound());
    }

    @Test
    public void testAddMoreOtherDocuments() throws Exception
    {
        Collection<String> docs = new ArrayList<String>();
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"uuid\">c94641f4-6dcf-4a28-9833-f6ad1052cfd8</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">updateTranslator</field><field name=\"name\">Add/Update a translator</field><field name=\"desc\">Add/Update a translator</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"uuid\",\"name\":\"Translator ID\",\"desc\":\"Translator ID\",\"type\":\"String\"}],\"uuid\":\"c94641f4-6dcf-4a28-9833-f6ad1052cfd8\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"updateTranslator\",\"name\":\"Add/Update a translator\",\"desc\":\"Add/Update a translator\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">TransportDef</field><field name=\"uuid\">458724cf-202e-49c3-9863-0e9c176cc27a</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"name\">SNMP Manager</field><field name=\"desc\">This transport provides SNMP Manager functionality.</field><field name=\"json\">{\"TransportDef\":{\"uuid\":\"458724cf-202e-49c3-9863-0e9c176cc27a\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"name\":\"SNMP Manager\",\"version\":\"1.0.0.0\",\"desc\":\"This transport provides SNMP Manager functionality.\",\"author\":\"Ladd Asper\",\"helpText\":\"TBD\",\"numSchedulerThreads\":10,\"initializeInstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"defaultValue\":\"0.0.0.0\",\"key\":\"Trap IP Address\",\"name\":\"Trap IP Address\",\"desc\":\"Trap IP Address\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":65535.0,\"stepSize\":0.0,\"precision\":0,\"defaultValue\":\"162\",\"key\":\"Trap Port Number\",\"name\":\"Trap Port Number\",\"desc\":\"Trap Port Number\",\"type\":\"Number\"},{\"isSelected\":true,\"key\":\"Listen for traps\",\"name\":\"Listen for traps\",\"desc\":\"Listen for traps\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"start\",\"name\":\"start\",\"desc\":\"start\",\"type\":\"SimpleBool\"}],\"key\":\"initialize\",\"name\":\"Set configuration values and initialize this transport.\",\"desc\":\"Set configuration values and initialize this transport.\"}}}</field></doc></add>");
        URL url = new URL("http://localhost:" + port + "/solandra/" + otherIndexName + "/update?commit=true");

        writeDocs(docs, url);

        SolrQuery q = new SolrQuery().setQuery("*:*").addField("*").addField("score");

        try
        {
            QueryResponse r = otherClient.query(q);

            assertEquals(29, r.getResults().getNumFound());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Test
    public void testClearAllRecords() throws Exception
    {
        Collection<String> uuidList = new ArrayList<String>();
        uuidList.add("51c43d10-668a-4a7c-a92b-e38230a65827");
        uuidList.add("0bbfe967-8326-45a2-a2ea-eafe366cc56d");
        uuidList.add("1fbed205-8dcd-4eb3-8562-3bd55fb01759");
        uuidList.add("889c7b35-a5c0-4692-b9a1-5eacc33c9c46");
        uuidList.add("753fdc94-04f4-4be6-abf4-028363acddfb");
        uuidList.add("3c4752c1-9fc3-4af1-afaf-749307adb419");
        uuidList.add("5584c6ee-be2f-4ce4-95b0-227638e971ad");
        uuidList.add("65edf22a-0290-4517-9de5-9ea198093a13");
        uuidList.add("63fcbe39-dc69-4279-b942-e3ff2630cf55");
        uuidList.add("652e7f6f-00a8-4fc5-a187-7a5fe923ef0e");
        uuidList.add("d3cebc10-ed42-4ee6-9b15-523b4f2c0109");
        uuidList.add("23685857-3daa-41ea-bdbe-8d0a69ab2742");
        uuidList.add("1d751043-bff1-4ce6-9145-c28aeb6439df");
        uuidList.add("a36bf16e-b94e-427d-b78e-eb37f2263720");
        uuidList.add("f04d7224-3190-4379-84e8-53d7034d8a4f");
        uuidList.add("08bbf4b6-70b8-4da4-a3af-f75ed30a246d");
        uuidList.add("4a15da95-f8aa-4039-ba49-f057d23004fa");
        uuidList.add("90a92cd3-3e95-47d6-a3b0-a86c2f298eac");
        uuidList.add("90a817a7-6812-4cfc-a4dd-a78718971b1b");
        uuidList.add("5c87542d-bcf8-401e-b5ff-ec4b324a7ddf");
        uuidList.add("9e834293-ae7f-4b79-8631-5daa40c1cee1");
        uuidList.add("a4b29317-5c85-4b15-a57f-4c816235f70f");
        uuidList.add("7028ad70-5d5d-435d-9573-39078b12ff69");
        uuidList.add("37c533aa-fd2a-4f43-af28-e4970262053b");
        uuidList.add("9186e0bc-d862-45d5-a461-a09aa52034ca");
        uuidList.add("54a74006-dfc6-4562-8cc0-4d30e78d6eb8");
        uuidList.add("29c9d1e1-86d0-499f-801a-bd03618deb35");
        uuidList.add("c94641f4-6dcf-4a28-9833-f6ad1052cfd8");
        uuidList.add("458724cf-202e-49c3-9863-0e9c176cc27a");

        Iterator<String> it = uuidList.iterator();
        while (it.hasNext())
        {
            otherClient.deleteById(it.next());
        }
            otherClient.commit();

        SolrQuery q = new SolrQuery().setQuery("*:*");

        QueryResponse r = otherClient.query(q);
        assertEquals(0, r.getResults().getNumFound());

    }

    @Test
    public void testAddMoreDocuments() throws Exception
    {
        Collection<String> docs = new ArrayList<String>();
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"generatorUUID\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"key\">start</field><field name=\"name\">Start this transport.</field><field name=\"desc\">Start this transport.</field><field name=\"uuid\">bfd683ea-2b72-44c0-963a-6cfa7417beaa</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"bfd683ea-2b72-44c0-963a-6cfa7417beaa\",\"ownerUUID\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"generatorUUID\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"key\":\"start\",\"name\":\"Start this transport.\",\"desc\":\"Start this transport.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"generatorUUID\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"key\">initialize</field><field name=\"name\">Set configuration values and initialize this transport.</field><field name=\"desc\">Set configuration values and initialize this transport.</field><field name=\"uuid\">2ec364fc-ebe2-45b6-9f56-7753e3c1dff4</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"defaultValue\":\"0.0.0.0\",\"key\":\"Trap IP Address\",\"name\":\"Trap IP Address\",\"desc\":\"Trap IP Address\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":65535.0,\"stepSize\":0.0,\"precision\":0,\"defaultValue\":\"162\",\"key\":\"Trap Port Number\",\"name\":\"Trap Port Number\",\"desc\":\"Trap Port Number\",\"type\":\"Number\"},{\"isSelected\":true,\"key\":\"Listen for traps\",\"name\":\"Listen for traps\",\"desc\":\"Listen for traps\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"start\",\"name\":\"start\",\"desc\":\"start\",\"type\":\"SimpleBool\"}],\"uuid\":\"2ec364fc-ebe2-45b6-9f56-7753e3c1dff4\",\"ownerUUID\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"generatorUUID\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"key\":\"initialize\",\"name\":\"Set configuration values and initialize this transport.\",\"desc\":\"Set configuration values and initialize this transport.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">delResource</field><field name=\"name\">Delete a resource</field><field name=\"desc\">Delete a resource</field><field name=\"uuid\">1c647f9c-e20d-43a5-81d2-d9e36702f65a</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"uuid\",\"name\":\"Resource ID\",\"desc\":\"Resource ID\",\"type\":\"String\"}],\"uuid\":\"1c647f9c-e20d-43a5-81d2-d9e36702f65a\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"delResource\",\"name\":\"Delete a resource\",\"desc\":\"Delete a resource\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">some key</field><field name=\"name\">My new datapoint</field><field name=\"uuid\">8f405a95-2f9c-42a7-9ace-153a9e3d2bde</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"sevLevel\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\",\"defaultValue\":\"0\"},\"resName\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"resName\",\"name\":\"Resource Name\",\"desc\":\"\"}},\"instructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":10,\"key\":\"some key\",\"name\":\"My new datapoint\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":255.0,\"stepSize\":0.0,\"precision\":0,\"key\":\"sevLevel\",\"name\":\"severity level\",\"type\":\"Number\"},{\"minLength\":0,\"maxLength\":10,\"key\":\"resName\",\"name\":\"Resource Name\",\"type\":\"String\"}],\"key\":\"some key\",\"name\":\"My new datapoint\",\"desc\":\"\"},\"isMap\":false,\"uuid\":\"8f405a95-2f9c-42a7-9ace-153a9e3d2bde\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"some key\",\"name\":\"My new datapoint\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">stop</field><field name=\"name\">Stop the Resource Manager</field><field name=\"desc\">Stop the Resource Manager</field><field name=\"uuid\">ceeb0980-f585-4f35-92e1-101ecae71656</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"ceeb0980-f585-4f35-92e1-101ecae71656\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"stop\",\"name\":\"Stop the Resource Manager\",\"desc\":\"Stop the Resource Manager\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">maintMode</field><field name=\"name\">Maintenance Mode</field><field name=\"uuid\">c73d6aac-a937-4cbe-bf0d-b65e82c258b3</field><field name=\"json\">{\"DatapointDef\":{\"instructionDef\":{\"parameterDefs\":[{\"isSelected\":true,\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"type\":\"SimpleBool\"}],\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"desc\":\"\"},\"isMap\":false,\"uuid\":\"c73d6aac-a937-4cbe-bf0d-b65e82c258b3\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">restart</field><field name=\"name\">Stop and Restart the Resource Manager</field><field name=\"desc\">Stop and Restart the Resource Manager</field><field name=\"uuid\">4a15da95-f8aa-4039-ba49-f057d23004fa</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"4a15da95-f8aa-4039-ba49-f057d23004fa\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"restart\",\"name\":\"Stop and Restart the Resource Manager\",\"desc\":\"Stop and Restart the Resource Manager\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">updateSharedModule</field><field name=\"name\">Add/Update a shared translator module</field><field name=\"desc\">Add/Update a shared translator module</field><field name=\"uuid\">90a92cd3-3e95-47d6-a3b0-a86c2f298eac</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"uuid\",\"name\":\"Shared Module ID\",\"desc\":\"Shared Module ID\",\"type\":\"String\"}],\"uuid\":\"90a92cd3-3e95-47d6-a3b0-a86c2f298eac\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"updateSharedModule\",\"name\":\"Add/Update a shared translator module\",\"desc\":\"Add/Update a shared translator module\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">stop</field><field name=\"name\">Overridden stop instruction</field><field name=\"desc\">Overridden stop instruction</field><field name=\"uuid\">65edf22a-0290-4517-9de5-9ea198093a13</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"65edf22a-0290-4517-9de5-9ea198093a13\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"stop\",\"name\":\"Overridden stop instruction\",\"desc\":\"Overridden stop instruction\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">start</field><field name=\"name\">Default start instruction.</field><field name=\"desc\">Default start instruction.</field><field name=\"uuid\">63fcbe39-dc69-4279-b942-e3ff2630cf55</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"63fcbe39-dc69-4279-b942-e3ff2630cf55\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"start\",\"name\":\"Default start instruction.\",\"desc\":\"Default start instruction.\"}}</field></doc></add>");

        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">delTranslator</field><field name=\"name\">Delete a translator</field><field name=\"desc\">Delete a translator</field><field name=\"uuid\">90a817a7-6812-4cfc-a4dd-a78718971b1b</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"translatorUUID\",\"name\":\"Translator ID\",\"desc\":\"Translator ID\",\"type\":\"String\"}],\"uuid\":\"90a817a7-6812-4cfc-a4dd-a78718971b1b\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"delTranslator\",\"name\":\"Delete a translator\",\"desc\":\"Delete a translator\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">setPollRate2</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">652e7f6f-00a8-4fc5-a187-7a5fe923ef0e</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"652e7f6f-00a8-4fc5-a187-7a5fe923ef0e\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"setPollRate2\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">setPollRate</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">d3cebc10-ed42-4ee6-9b15-523b4f2c0109</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"d3cebc10-ed42-4ee6-9b15-523b4f2c0109\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"setPollRate\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">675c7858-631a-4ce3-bee6-5312aa3c4b1b</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">initialize</field><field name=\"name\">Inititialize the resource with the following parameters.</field><field name=\"desc\">Inititialize the resource with the following parameters.</field><field name=\"uuid\">23685857-3daa-41ea-bdbe-8d0a69ab2742</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":20,\"key\":\"A\",\"name\":\"Position Name\",\"desc\":\"Position Name\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.01,\"precision\":2,\"units\":\"seconds\",\"key\":\"B\",\"name\":\"UTC Time\",\"desc\":\"UTC Time\",\"type\":\"Number\"},{\"listEntries\":[\"Clockwise\",\"Counterclockwise\",\"Shortest Path\"],\"selectedEntries\":[],\"key\":\"C\",\"name\":\"Cable Wrap\",\"desc\":\"Cable Wrap\",\"type\":\"List\"},{\"key\":\"D\",\"name\":\"Some File Parm\",\"desc\":\"Desc for some file parm\",\"type\":\"File\"},{\"options\":[{\"isSelected\":false,\"key\":\"Pick Me\",\"name\":\"Pick Me\",\"desc\":\"Pick Me\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"},\"key\":\"E\",\"name\":\"A group bool parm\",\"desc\":\"A group bool parm\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"23685857-3daa-41ea-bdbe-8d0a69ab2742\",\"ownerUUID\":\"675c7858-631a-4ce3-bee6-5312aa3c4b1b\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"initialize\",\"name\":\"Inititialize the resource with the following parameters.\",\"desc\":\"Inititialize the resource with the following parameters.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">some key</field><field name=\"name\">My new datapoint</field><field name=\"uuid\">08bbf4b6-70b8-4da4-a3af-f75ed30a246d</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"sevLevel\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\",\"defaultValue\":\"0\"},\"resName\":{\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"resName\",\"name\":\"Resource Name\",\"desc\":\"\"}},\"instructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":10,\"key\":\"some key\",\"name\":\"My new datapoint\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":255.0,\"stepSize\":0.0,\"precision\":0,\"key\":\"sevLevel\",\"name\":\"severity level\",\"type\":\"Number\"},{\"minLength\":0,\"maxLength\":10,\"key\":\"resName\",\"name\":\"Resource Name\",\"type\":\"String\"}],\"key\":\"some key\",\"name\":\"My new datapoint\",\"desc\":\"\"},\"isMap\":false,\"uuid\":\"08bbf4b6-70b8-4da4-a3af-f75ed30a246d\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"some key\",\"name\":\"My new datapoint\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">delTransport</field><field name=\"name\">Delete a transport</field><field name=\"desc\">Delete a transport</field><field name=\"uuid\">0cf0a9fa-772d-4294-b8a7-e42d8679d059</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"transportUUID\",\"name\":\"Transport ID\",\"desc\":\"Transport ID\",\"type\":\"String\"}],\"uuid\":\"0cf0a9fa-772d-4294-b8a7-e42d8679d059\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"delTransport\",\"name\":\"Delete a transport\",\"desc\":\"Delete a transport\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">maintMode</field><field name=\"name\">Maintenance Mode</field><field name=\"uuid\">f3a89310-6907-4530-b6a9-942663eda05e</field><field name=\"json\">{\"DatapointDef\":{\"instructionDef\":{\"parameterDefs\":[{\"isSelected\":true,\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"type\":\"SimpleBool\"}],\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"desc\":\"\"},\"isMap\":false,\"uuid\":\"f3a89310-6907-4530-b6a9-942663eda05e\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">delSharedModule</field><field name=\"name\">Delete a shared translator module</field><field name=\"desc\">Delete a shared translator module</field><field name=\"uuid\">50771f7c-1c08-451c-83cc-9e3975c2f94e</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"uuid\",\"name\":\"Shared Module ID\",\"desc\":\"Shared Module ID\",\"type\":\"String\"}],\"uuid\":\"50771f7c-1c08-451c-83cc-9e3975c2f94e\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"delSharedModule\",\"name\":\"Delete a shared translator module\",\"desc\":\"Delete a shared translator module\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">addTransport</field><field name=\"name\">Add a transport</field><field name=\"desc\">Add a transport</field><field name=\"uuid\">57e96bda-fb02-47e7-898a-4db2243d4e23</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"key\":\"name\",\"name\":\"Name\",\"desc\":\"Name\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":0,\"key\":\"desc\",\"name\":\"Description\",\"desc\":\"Description\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":36,\"key\":\"transportTypeUUID\",\"name\":\"Transport Type ID\",\"desc\":\"Transport Type ID\",\"type\":\"String\"},{\"isSelected\":true,\"key\":\"initialize\",\"name\":\"Call the transport\u0027s initialize() method?\",\"desc\":\"Call the transport\u0027s initialize() method?\",\"type\":\"SimpleBool\"}],\"uuid\":\"57e96bda-fb02-47e7-898a-4db2243d4e23\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"addTransport\",\"name\":\"Add a transport\",\"desc\":\"Add a transport\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">stop</field><field name=\"name\">Overridden stop instruction</field><field name=\"desc\">Overridden stop instruction</field><field name=\"uuid\">39635c77-134f-423c-8b56-23b5f550f4c0</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"39635c77-134f-423c-8b56-23b5f550f4c0\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"stop\",\"name\":\"Overridden stop instruction\",\"desc\":\"Overridden stop instruction\"}}</field></doc></add>");

        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">start</field><field name=\"name\">Default start instruction.</field><field name=\"desc\">Default start instruction.</field><field name=\"uuid\">b5c9a652-10e6-4676-8a41-06058670d9f6</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"b5c9a652-10e6-4676-8a41-06058670d9f6\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"start\",\"name\":\"Default start instruction.\",\"desc\":\"Default start instruction.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">setPollRate2</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">c73f225e-8d58-4d93-bdbf-f8f8eb0693f7</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"c73f225e-8d58-4d93-bdbf-f8f8eb0693f7\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"setPollRate2\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">setPollRate</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">32a7ddc3-fb7d-4b5f-af83-d309a5c0e09a</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"32a7ddc3-fb7d-4b5f-af83-d309a5c0e09a\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"setPollRate\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">1d751043-bff1-4ce6-9145-c28aeb6439df</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">initialize</field><field name=\"name\">Inititialize the resource with the following parameters.</field><field name=\"desc\">Inititialize the resource with the following parameters.</field><field name=\"uuid\">d6f587b2-46b8-4c6b-9495-9557bc66f61c</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":20,\"key\":\"A\",\"name\":\"Position Name\",\"desc\":\"Position Name\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.01,\"precision\":2,\"units\":\"seconds\",\"key\":\"B\",\"name\":\"UTC Time\",\"desc\":\"UTC Time\",\"type\":\"Number\"},{\"listEntries\":[\"Clockwise\",\"Counterclockwise\",\"Shortest Path\"],\"selectedEntries\":[],\"key\":\"C\",\"name\":\"Cable Wrap\",\"desc\":\"Cable Wrap\",\"type\":\"List\"},{\"key\":\"D\",\"name\":\"Some File Parm\",\"desc\":\"Desc for some file parm\",\"type\":\"File\"},{\"options\":[{\"isSelected\":false,\"key\":\"Pick Me\",\"name\":\"Pick Me\",\"desc\":\"Pick Me\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"},\"key\":\"E\",\"name\":\"A group bool parm\",\"desc\":\"A group bool parm\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"d6f587b2-46b8-4c6b-9495-9557bc66f61c\",\"ownerUUID\":\"1d751043-bff1-4ce6-9145-c28aeb6439df\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"initialize\",\"name\":\"Inititialize the resource with the following parameters.\",\"desc\":\"Inititialize the resource with the following parameters.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">moveResource</field><field name=\"name\">Move a resource to a new transport</field><field name=\"desc\">Move a resource to a new transport</field><field name=\"uuid\">2e991425-ef86-429a-b801-1a3e5da6a134</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"transportTypeUUID\",\"name\":\"Transport Type ID\",\"desc\":\"Transport Type ID\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":36,\"key\":\"transportUUID\",\"name\":\"Transport ID\",\"desc\":\"Transport ID\",\"type\":\"String\"}],\"uuid\":\"2e991425-ef86-429a-b801-1a3e5da6a134\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"moveResource\",\"name\":\"Move a resource to a new transport\",\"desc\":\"Move a resource to a new transport\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">addResource</field><field name=\"name\">Add a resource</field><field name=\"desc\">Add a resource</field><field name=\"uuid\">caedc0d1-2719-49ae-b3da-fdac3aef6bd7</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"key\":\"name\",\"name\":\"Name\",\"desc\":\"Name\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":0,\"key\":\"desc\",\"name\":\"Description\",\"desc\":\"Description\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":36,\"key\":\"transportTypeUUID\",\"name\":\"Transport Type ID\",\"desc\":\"Transport Type ID\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":36,\"key\":\"transportUUID\",\"name\":\"Transport ID\",\"desc\":\"Transport ID\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":36,\"key\":\"translatorUUID\",\"name\":\"Translator ID\",\"desc\":\"Translator ID\",\"type\":\"String\"},{\"isSelected\":true,\"key\":\"initialize\",\"name\":\"Call the translator\u0027s initialize() method for this resource?\",\"desc\":\"Call the translator\u0027s initialize() method for this resource?\",\"type\":\"SimpleBool\"}],\"uuid\":\"caedc0d1-2719-49ae-b3da-fdac3aef6bd7\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"addResource\",\"name\":\"Add a resource\",\"desc\":\"Add a resource\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9</field><field name=\"generatorUUID\">066f609d-e65b-4a51-ac52-296218aece8d</field><field name=\"key\">maintMode</field><field name=\"name\">Maintenance Mode</field><field name=\"uuid\">6787257b-0b11-402f-a3e8-788e171069bc</field><field name=\"json\">{\"DatapointDef\":{\"instructionDef\":{\"parameterDefs\":[{\"isSelected\":true,\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"type\":\"SimpleBool\"}],\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"desc\":\"\"},\"isMap\":false,\"uuid\":\"6787257b-0b11-402f-a3e8-788e171069bc\",\"ownerUUID\":\"a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9\",\"generatorUUID\":\"066f609d-e65b-4a51-ac52-296218aece8d\",\"key\":\"maintMode\",\"name\":\"Maintenance Mode\",\"desc\":\"\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">TransportDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"name\">SNMP Manager</field><field name=\"desc\">This transport provides SNMP Manager functionality.</field><field name=\"uuid\">458724cf-202e-49c3-9863-0e9c176cc27a</field><field name=\"json\">{\"TransportDef\":{\"uuid\":\"458724cf-202e-49c3-9863-0e9c176cc27a\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"name\":\"SNMP Manager\",\"version\":\"1.0.0.0\",\"desc\":\"This transport provides SNMP Manager functionality.\",\"author\":\"Ladd Asper\",\"helpText\":\"TBD\",\"numSchedulerThreads\":10,\"initializeInstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"defaultValue\":\"0.0.0.0\",\"key\":\"Trap IP Address\",\"name\":\"Trap IP Address\",\"desc\":\"Trap IP Address\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":65535.0,\"stepSize\":0.0,\"precision\":0,\"defaultValue\":\"162\",\"key\":\"Trap Port Number\",\"name\":\"Trap Port Number\",\"desc\":\"Trap Port Number\",\"type\":\"Number\"},{\"isSelected\":true,\"key\":\"Listen for traps\",\"name\":\"Listen for traps\",\"desc\":\"Listen for traps\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"start\",\"name\":\"start\",\"desc\":\"start\",\"type\":\"SimpleBool\"}],\"key\":\"initialize\",\"name\":\"Set configuration values and initialize this transport.\",\"desc\":\"Set configuration values and initialize this transport.\"}}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9</field><field name=\"generatorUUID\">066f609d-e65b-4a51-ac52-296218aece8d</field><field name=\"key\">stop</field><field name=\"name\">Default stop instruction.</field><field name=\"desc\">Default stop instruction.</field><field name=\"uuid\">7bfe182e-91e2-4023-b01c-1c0228cc9d3e</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"7bfe182e-91e2-4023-b01c-1c0228cc9d3e\",\"ownerUUID\":\"a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9\",\"generatorUUID\":\"066f609d-e65b-4a51-ac52-296218aece8d\",\"key\":\"stop\",\"name\":\"Default stop instruction.\",\"desc\":\"Default stop instruction.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9</field><field name=\"generatorUUID\">066f609d-e65b-4a51-ac52-296218aece8d</field><field name=\"key\">start</field><field name=\"name\">Default start instruction.</field><field name=\"desc\">Default start instruction.</field><field name=\"uuid\">9d876698-e2ad-4adb-9124-70b1c4b034f2</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"9d876698-e2ad-4adb-9124-70b1c4b034f2\",\"ownerUUID\":\"a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9\",\"generatorUUID\":\"066f609d-e65b-4a51-ac52-296218aece8d\",\"key\":\"start\",\"name\":\"Default start instruction.\",\"desc\":\"Default start instruction.\"}}</field></doc></add>");

        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">TransportDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"name\">TCP Client</field><field name=\"desc\">This is a general purpose TCP client transport.</field><field name=\"uuid\">550e8400-e29b-41d4-a716-446655440002</field><field name=\"json\">{\"TransportDef\":{\"uuid\":\"550e8400-e29b-41d4-a716-446655440002\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"name\":\"TCP Client\",\"version\":\"1.0.0.0\",\"desc\":\"This is a general purpose TCP client transport.\",\"author\":\"Ladd Asper\",\"helpText\":\"TBD\",\"numSchedulerThreads\":1,\"initializeInstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"defaultValue\":\"127.0.0.1\",\"key\":\"IP Address\",\"name\":\"IP Address\",\"desc\":\"IP Address\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":65535.0,\"stepSize\":0.0,\"precision\":0,\"defaultValue\":\"54321\",\"key\":\"TCP Port Number\",\"name\":\"TCP Port Number\",\"desc\":\"TCP Port Number\",\"type\":\"Number\"},{\"options\":[{\"isSelected\":true,\"key\":\"IPv4\",\"name\":\"IPv4\",\"desc\":\"IPv4\",\"type\":\"SimpleBool\"},{\"isSelected\":false,\"key\":\"IPv6\",\"name\":\"IPv6\",\"desc\":\"IPv6\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"IPv4\",\"name\":\"IPv4\",\"desc\":\"IPv4\",\"type\":\"SimpleBool\"},\"key\":\"IP Version\",\"name\":\"IP Version\",\"desc\":\"IP Version\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"start\",\"name\":\"start\",\"desc\":\"start\",\"type\":\"SimpleBool\"}],\"key\":\"initialize\",\"name\":\"Set configuration values and initialize this transport.\",\"desc\":\"Set configuration values and initialize this transport.\"}}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9</field><field name=\"generatorUUID\">066f609d-e65b-4a51-ac52-296218aece8d</field><field name=\"key\">setPollRate</field><field name=\"name\">Sets the poll rate to a new value in milliseconds.</field><field name=\"desc\">Sets the poll rate to a new value in milliseconds.</field><field name=\"uuid\">fd20f7e6-95b2-445b-b0cb-c42a03c29efd</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.0,\"precision\":0,\"units\":\"ms\",\"key\":\"newRate\",\"name\":\"New Poll Rate\",\"desc\":\"New Poll Rate\",\"type\":\"Number\"}],\"uuid\":\"fd20f7e6-95b2-445b-b0cb-c42a03c29efd\",\"ownerUUID\":\"a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9\",\"generatorUUID\":\"066f609d-e65b-4a51-ac52-296218aece8d\",\"key\":\"setPollRate\",\"name\":\"Sets the poll rate to a new value in milliseconds.\",\"desc\":\"Sets the poll rate to a new value in milliseconds.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9</field><field name=\"generatorUUID\">066f609d-e65b-4a51-ac52-296218aece8d</field><field name=\"key\">initialize</field><field name=\"name\">Inititialize the resource with the following parameters.</field><field name=\"desc\">Inititialize the resource with the following parameters.</field><field name=\"uuid\">ce99fdc8-61a5-4108-bcfd-13dbaff41866</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":20,\"key\":\"A\",\"name\":\"Position Name\",\"desc\":\"Position Name\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":86400.0,\"stepSize\":0.01,\"precision\":2,\"units\":\"seconds\",\"key\":\"B\",\"name\":\"UTC Time\",\"desc\":\"UTC Time\",\"type\":\"Number\"},{\"listEntries\":[\"Clockwise\",\"Counterclockwise\",\"Shortest Path\"],\"selectedEntries\":[],\"key\":\"C\",\"name\":\"Cable Wrap\",\"desc\":\"Cable Wrap\",\"type\":\"List\"},{\"key\":\"D\",\"name\":\"Some File Parm\",\"desc\":\"Desc for some file parm\",\"type\":\"File\"},{\"options\":[{\"isSelected\":false,\"key\":\"Pick Me\",\"name\":\"Pick Me\",\"desc\":\"Pick Me\",\"type\":\"SimpleBool\"},{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"No, pick me\",\"name\":\"No, pick me\",\"desc\":\"Sometimes pick this guy\",\"type\":\"SimpleBool\"},\"key\":\"E\",\"name\":\"A group bool parm\",\"desc\":\"A group bool parm\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"F\",\"name\":\"A simple bool parm\",\"desc\":\"Desc for simple bool parm\",\"type\":\"SimpleBool\"}],\"uuid\":\"ce99fdc8-61a5-4108-bcfd-13dbaff41866\",\"ownerUUID\":\"a4e0913b-5d1d-4fd0-9af1-fb75727ee2a9\",\"generatorUUID\":\"066f609d-e65b-4a51-ac52-296218aece8d\",\"key\":\"initialize\",\"name\":\"Inititialize the resource with the following parameters.\",\"desc\":\"Inititialize the resource with the following parameters.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">TransportDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"name\">Generic Transport</field><field name=\"desc\">This is a general purpose transport.</field><field name=\"uuid\">8713d67c-4dbc-45ef-b9d4-6d4d4772a7a3</field><field name=\"json\">{\"TransportDef\":{\"uuid\":\"8713d67c-4dbc-45ef-b9d4-6d4d4772a7a3\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"name\":\"Generic Transport\",\"version\":\"1.0.0.0\",\"desc\":\"This is a general purpose transport.\",\"author\":\"Ladd Asper\",\"helpText\":\"TBD\",\"numSchedulerThreads\":1,\"initializeInstructionDef\":{\"parameterDefs\":[{\"minValue\":0.0,\"maxValue\":65535.0,\"stepSize\":0.0,\"precision\":0,\"defaultValue\":\"12345\",\"key\":\"someNumberParm\",\"name\":\"and this is a number parm\",\"desc\":\"and this is a number parm\",\"type\":\"Number\"},{\"isSelected\":true,\"key\":\"start\",\"name\":\"start\",\"desc\":\"start\",\"type\":\"SimpleBool\"}],\"key\":\"initialize\",\"name\":\"Set configuration values and initialize this transport.\",\"desc\":\"Set configuration values and initialize this transport.\"}}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"generatorUUID\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"key\">stop</field><field name=\"name\">Stop this transport.</field><field name=\"desc\">Stop this transport.</field><field name=\"uuid\">65ce3138-5ae4-4914-a682-cda5b6275588</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"65ce3138-5ae4-4914-a682-cda5b6275588\",\"ownerUUID\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"generatorUUID\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"key\":\"stop\",\"name\":\"Stop this transport.\",\"desc\":\"Stop this transport.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">DatapointDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">rmDpKey3</field><field name=\"name\">A third Resource Manager Datapoint</field><field name=\"desc\">A settable Resource Manager Datapoint!!</field><field name=\"uuid\">8538baed-056f-4f26-b69f-b257dbee85e5</field><field name=\"json\">{\"DatapointDef\":{\"attributeDefs\":{\"sevLevel\":{\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"sevLevel\",\"name\":\"severity level\",\"desc\":\"\",\"defaultValue\":\"0\"},\"config\":{\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"config\",\"name\":\"config\",\"desc\":\"\"}},\"instructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":20,\"key\":\"rmDpKey3\",\"name\":\"A third Resource Manager Datapoint\",\"type\":\"String\"},{\"minLength\":0,\"maxLength\":20,\"key\":\"sevLevel\",\"name\":\"severity level\",\"type\":\"String\"},{\"isSelected\":true,\"key\":\"config\",\"name\":\"config\",\"type\":\"SimpleBool\"}],\"key\":\"rmDpKey3\",\"name\":\"A third Resource Manager Datapoint\",\"desc\":\"A settable Resource Manager Datapoint!!\"},\"isMap\":false,\"uuid\":\"8538baed-056f-4f26-b69f-b257dbee85e5\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"rmDpKey3\",\"name\":\"A third Resource Manager Datapoint\",\"desc\":\"A settable Resource Manager Datapoint!!\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"generatorUUID\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"key\">start</field><field name=\"name\">Start this transport.</field><field name=\"desc\">Start this transport.</field><field name=\"uuid\">4d3074ba-d3b0-4331-b126-b24e2f6c9bf3</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"4d3074ba-d3b0-4331-b126-b24e2f6c9bf3\",\"ownerUUID\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"generatorUUID\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"key\":\"start\",\"name\":\"Start this transport.\",\"desc\":\"Start this transport.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">9186e0bc-d862-45d5-a461-a09aa52034ca</field><field name=\"generatorUUID\">9186e0bc-d862-45d5-a461-a09aa52034ca</field><field name=\"key\">stop</field><field name=\"name\">Stop this transport.</field><field name=\"desc\">Stop this transport.</field><field name=\"uuid\">362da90b-c988-4348-9b90-5a7696129dd2</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"362da90b-c988-4348-9b90-5a7696129dd2\",\"ownerUUID\":\"9186e0bc-d862-45d5-a461-a09aa52034ca\",\"generatorUUID\":\"9186e0bc-d862-45d5-a461-a09aa52034ca\",\"key\":\"stop\",\"name\":\"Stop this transport.\",\"desc\":\"Stop this transport.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">9186e0bc-d862-45d5-a461-a09aa52034ca</field><field name=\"generatorUUID\">9186e0bc-d862-45d5-a461-a09aa52034ca</field><field name=\"key\">start</field><field name=\"name\">Start this transport.</field><field name=\"desc\">Start this transport.</field><field name=\"uuid\">85a2c7c2-d621-4456-b021-a720f713d192</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"85a2c7c2-d621-4456-b021-a720f713d192\",\"ownerUUID\":\"9186e0bc-d862-45d5-a461-a09aa52034ca\",\"generatorUUID\":\"9186e0bc-d862-45d5-a461-a09aa52034ca\",\"key\":\"start\",\"name\":\"Start this transport.\",\"desc\":\"Start this transport.\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"generatorUUID\">54a74006-dfc6-4562-8cc0-4d30e78d6eb8</field><field name=\"key\">initialize</field><field name=\"name\">Set configuration values and initialize this transport.</field><field name=\"desc\">Set configuration values and initialize this transport.</field><field name=\"uuid\">b0496cb4-73ce-4774-9404-57489a0252f6</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":0,\"defaultValue\":\"127.0.0.1\",\"key\":\"IP Address\",\"name\":\"IP Address\",\"desc\":\"IP Address\",\"type\":\"String\"},{\"minValue\":0.0,\"maxValue\":65535.0,\"stepSize\":0.0,\"precision\":0,\"defaultValue\":\"54321\",\"key\":\"TCP Port Number\",\"name\":\"TCP Port Number\",\"desc\":\"TCP Port Number\",\"type\":\"Number\"},{\"options\":[{\"isSelected\":true,\"key\":\"IPv4\",\"name\":\"IPv4\",\"desc\":\"IPv4\",\"type\":\"SimpleBool\"},{\"isSelected\":false,\"key\":\"IPv6\",\"name\":\"IPv6\",\"desc\":\"IPv6\",\"type\":\"SimpleBool\"}],\"defaultSelectedOption\":{\"isSelected\":true,\"key\":\"IPv4\",\"name\":\"IPv4\",\"desc\":\"IPv4\",\"type\":\"SimpleBool\"},\"key\":\"IP Version\",\"name\":\"IP Version\",\"desc\":\"IP Version\",\"type\":\"GroupBool\"},{\"isSelected\":true,\"key\":\"start\",\"name\":\"start\",\"desc\":\"start\",\"type\":\"SimpleBool\"}],\"uuid\":\"b0496cb4-73ce-4774-9404-57489a0252f6\",\"ownerUUID\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"generatorUUID\":\"54a74006-dfc6-4562-8cc0-4d30e78d6eb8\",\"key\":\"initialize\",\"name\":\"Set configuration values and initialize this transport.\",\"desc\":\"Set configuration values and initialize this transport.\"}}</field></doc></add>");

        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"generatorUUID\">550e8400-e29b-41d4-a716-446655440001</field><field name=\"key\">updateTranslator</field><field name=\"name\">Add/Update a translator</field><field name=\"desc\">Add/Update a translator</field><field name=\"uuid\">8c188b18-ce6b-402b-b2e0-750bdf2e27ca</field><field name=\"json\">{\"InstructionDef\":{\"parameterDefs\":[{\"minLength\":0,\"maxLength\":36,\"key\":\"uuid\",\"name\":\"Translator ID\",\"desc\":\"Translator ID\",\"type\":\"String\"}],\"uuid\":\"8c188b18-ce6b-402b-b2e0-750bdf2e27ca\",\"ownerUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"generatorUUID\":\"550e8400-e29b-41d4-a716-446655440001\",\"key\":\"updateTranslator\",\"name\":\"Add/Update a translator\",\"desc\":\"Add/Update a translator\"}}</field></doc></add>");
        docs
                .add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">InstructionDef</field><field name=\"ownerUUID\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"generatorUUID\">29c9d1e1-86d0-499f-801a-bd03618deb35</field><field name=\"key\">stop</field><field name=\"name\">Stop this transport.</field><field name=\"desc\">Stop this transport.</field><field name=\"uuid\">867441f2-ff91-4bc4-aa28-a7c1c4900d02</field><field name=\"json\">{\"InstructionDef\":{\"uuid\":\"867441f2-ff91-4bc4-aa28-a7c1c4900d02\",\"ownerUUID\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"generatorUUID\":\"29c9d1e1-86d0-499f-801a-bd03618deb35\",\"key\":\"stop\",\"name\":\"Stop this transport.\",\"desc\":\"Stop this transport.\"}}</field></doc></add>");

        URL url = new URL("http://localhost:" + port + "/solandra/~" + otherIndexName + "/update?commit=true");

        writeDocs(docs, url);

        SolrQuery q = new SolrQuery().setQuery("*:*");

        try
        {
            QueryResponse r = otherClient.query(q);
            assertEquals(42, r.getResults().getNumFound());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private void writeDocs(Collection<String> docs, URL url)
    {
        // write
        try
        {
            Iterator<String> it = docs.iterator();
            while (it.hasNext())
            {
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestProperty("Content-Type", "text/xml");
                conn.setDoOutput(true);

                OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
                wr.write(it.next());
                wr.flush();
                wr.close();
                assertEquals(200, conn.getResponseCode());
            }

        }
        catch (IOException e)
        {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSingleSearchQuery() throws Exception
    {

        SolrQuery     q = new SolrQuery().setQuery("key:stop");
        QueryResponse r = otherClient.query(q);

        assertEquals(7, r.getResults().getNumFound());

        q = new SolrQuery().setQuery("messageType:[* TO *]");

        r = otherClient.query(q);

        assertEquals(42, r.getResults().getNumFound());
    }

    @Test
    public void testDualSearchQuery() throws Exception
    {

        SolrQuery q = new SolrQuery().setQuery("messageType:InstructionDef AND key:stop");

        QueryResponse r = otherClient.query(q);

        assertEquals(7, r.getResults().getNumFound());
    }

    @Test
    public void testLongRangeQueries() throws Exception
    {
        Collection<String> docs = Arrays.asList(
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Datapoint</field><field name=\"uuid\">002ab136-a6b4-42b1-af78-7d87bf133a60</field><field name=\"ownerUUID\">4941f74b-2b50-4ea5-9f0e-0a49ed11adb0</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp6</field><field name=\"name\">Generated datapoint 6</field><field name=\"modTime\">1300214308085</field><field name=\"json\">{\"Datapoint\":{\"uuid\":\"002ab136-a6b4-42b1-af78-7d87bf133a60\",\"ownerUUID\":\"4941f74b-2b50-4ea5-9f0e-0a49ed11adb0\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp6\",\"name\":\"Generated datapoint 6\",\"value\":\"11\",\"modTime\":1300214308085}}</field></doc></add>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Datapoint</field><field name=\"uuid\">002ab136-a6b4-42b1-af78-7d87bf133a61</field><field name=\"ownerUUID\">4941f74b-2b50-4ea5-9f0e-0a49ed11adb0</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp6</field><field name=\"name\">Generated datapoint 6</field><field name=\"modTime\">1300214313085</field><field name=\"json\">{\"Datapoint\":{\"uuid\":\"002ab136-a6b4-42b1-af78-7d87bf133a61\",\"ownerUUID\":\"4941f74b-2b50-4ea5-9f0e-0a49ed11adb0\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp6\",\"name\":\"Generated datapoint 6\",\"value\":\"13\",\"modTime\":1300214313085}}</field></doc></add>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Datapoint</field><field name=\"uuid\">002ab136-a6b4-42b1-af78-7d87bf133a62</field><field name=\"ownerUUID\">4941f74b-2b50-4ea5-9f0e-0a49ed11adb0</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp6</field><field name=\"name\">Generated datapoint 6</field><field name=\"modTime\">1300214318085</field><field name=\"json\">{\"Datapoint\":{\"uuid\":\"002ab136-a6b4-42b1-af78-7d87bf133a62\",\"ownerUUID\":\"4941f74b-2b50-4ea5-9f0e-0a49ed11adb0\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp6\",\"name\":\"Generated datapoint 6\",\"value\":\"18\",\"modTime\":1300214318085}}</field></doc></add>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Datapoint</field><field name=\"uuid\">002ab136-a6b4-42b1-af78-7d87bf133a63</field><field name=\"ownerUUID\">4941f74b-2b50-4ea5-9f0e-0a49ed11adb0</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp6</field><field name=\"name\">Generated datapoint 6</field><field name=\"modTime\">1300214323085</field><field name=\"json\">{\"Datapoint\":{\"uuid\":\"002ab136-a6b4-42b1-af78-7d87bf133a63\",\"ownerUUID\":\"4941f74b-2b50-4ea5-9f0e-0a49ed11adb0\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp6\",\"name\":\"Generated datapoint 6\",\"value\":\"19\",\"modTime\":1300214323085}}</field></doc></add>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><add><doc><field name=\"messageType\">Datapoint</field><field name=\"uuid\">002ab136-a6b4-42b1-af78-7d87bf133a64</field><field name=\"ownerUUID\">4941f74b-2b50-4ea5-9f0e-0a49ed11adb0</field><field name=\"generatorUUID\">ff1df93f-a19f-4088-97e9-14f165aacab4</field><field name=\"key\">dp6</field><field name=\"name\">Generated datapoint 6</field><field name=\"modTime\">1300214328085</field><field name=\"json\">{\"Datapoint\":{\"uuid\":\"002ab136-a6b4-42b1-af78-7d87bf133a64\",\"ownerUUID\":\"4941f74b-2b50-4ea5-9f0e-0a49ed11adb0\",\"generatorUUID\":\"ff1df93f-a19f-4088-97e9-14f165aacab4\",\"key\":\"dp6\",\"name\":\"Generated datapoint 6\",\"value\":\"14\",\"modTime\":1300214328085}}</field></doc></add>"
            );
        URL url = new URL("http://localhost:" + port + "/solandra/" + otherIndexName + "/update?commit=true&batch=true");

        writeDocs(docs, url);

        SolrQuery q;
        QueryResponse r;
        
        q = new SolrQuery().setQuery("messageType:[* TO *]");
        r = otherClient.query(q);
        assertEquals(47, r.getResults().getNumFound());

        q = new SolrQuery().setQuery("messageType:Datapoint");
        r = otherClient.query(q);
        assertEquals(5, r.getResults().getNumFound());

        // 1 ms after the first and before the last
        q = new SolrQuery().setQuery("messageType:Datapoint AND modTime:[1300214308086 TO 1300214328084] AND ownerUUID:4941f74b-2b50-4ea5-9f0e-0a49ed11adb0");
        q.setSortField("modTime", ORDER.desc);
        r = otherClient.query(q);
        assertEquals(3, r.getResults().getNumFound());
        assertTrue(((String)r.getResults().get(0).getFieldValue("json")).contains("1300214323085"));
    }

}
