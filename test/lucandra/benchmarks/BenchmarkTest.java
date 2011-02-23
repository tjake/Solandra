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
package lucandra.benchmarks;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

public class BenchmarkTest {

    private enum Type {
        read, write, both
    }

    private static int numClients = 20;
    private static int numLoops = 10;
    private static Type type = Type.both;
    private static String indexName = "bench";

    private static String text = "this is a benchmark of lucandra";
    private static String queryString = "text:benchmark";
    private static int threadId = 0;
    private static int port = 8983;
    private static String url = "http://localhost";
    private static String[] types = new String[]{"1","2","3","4","5","6","7","8","9","10"};
    private static Random  random = new Random(System.currentTimeMillis()); 
    private static CommonsHttpSolrServer streamingClient = null;
    
    private static Runnable getRunnable() {

        try {
            return new Runnable() {

                private CommonsHttpSolrServer solrClient;
                                
                private final SolrQuery q = new SolrQuery().setQuery(queryString).addFacetField("type").setSortField("id", ORDER.asc).addField("*");
                //private final SolrQuery q = new SolrQuery().setQuery(queryString).setSortField("id", ORDER.asc);
                //private final SolrQuery q = new SolrQuery().setQuery(queryString);
                
                private final int myThreadId = threadId++;
                
                private SolrInputDocument getDocument(){
                    SolrInputDocument doc =  new SolrInputDocument();
                    doc.addField("text", text);
                    doc.addField("type", types[random.nextInt(types.length-1)]);
                    doc.addField("id", ""+System.nanoTime()+Math.random());

                    return doc;
                }
                
                private CommonsHttpSolrServer getStreamingServer(String url) throws MalformedURLException
                {
                    
                    if(streamingClient == null)
                    {
                        synchronized (url.intern())
                        {
                            if(streamingClient == null)
                            {
                                streamingClient =  new StreamingUpdateSolrServer(url, 512, numClients);
                            }
                        }
                    }                 
                    
                    return streamingClient;
                }
                
                public void run() {
                    
                    try{
                        
                        String fullUrl;
                        
                        if(indexName.equals(""))
                            fullUrl = url + ":" + port +  "/solr";
                        else
                            fullUrl = url + ":" + port +  "/solandra/"+indexName;
                        
                        if(type == Type.write)
                            solrClient = getStreamingServer(fullUrl);
                        else
                            solrClient = new CommonsHttpSolrServer(fullUrl);
                        
                    }catch(MalformedURLException e){
                        
                    }
                    
                    try {
                        switch (type) {
                        case read:
                            read();
                            break;
                        case write:
                            write();
                            break;
                        default:
                            both();
                        }
                    } catch (SolrServerException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {

                    }
                }             

                private void read() throws SolrServerException {

                    long total = 0;

                    for (int i = 0; i < numLoops; i++) {

                        QueryResponse r = solrClient.query(q);

                        total = r.getResults().getNumFound();

                        if (i>0 && i % numLoops/2 == 0)
                            System.err.println("Thread " + myThreadId + ": total " + total + " facets "+r.getFacetFields());
                    }

                    if (myThreadId == 0)
                        System.err.println("Documents found: " + total);
                }

                private void write() throws SolrServerException, IOException {

                    for (int i = 0; i < numLoops; i++) {
                       
                        solrClient.add(getDocument());                      
                    }
                    
                    solrClient.commit(true,true);
                }

                private void both() throws SolrServerException, IOException {

                    long total = 0;

                    for (int i = 0; i < numLoops; i++) {

                        if (i % 2 == 1) {
                            QueryResponse r = solrClient.query(q);
                            total = r.getResults().getNumFound();

                            if (i > 1 && i % numLoops/2 == 1)
                                System.err.println("Thread " + myThreadId + ": total " + total + " facets " +r.getFacetFields());

                        } else {
                            
                            solrClient.add(getDocument());
                          
                        }

                    }

                    solrClient.commit(true,true);
                    
                    if (myThreadId == 0)
                        System.err.println("Documents found: " + total);
                }
            };
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    
    
    
    private static void usage() {

        System.err.print(BenchmarkTest.class.getSimpleName() + " [--clients=<client-count>] [--loops=<loop-count>] [--type=<test-type>]\n"
                + "\tclients        Number of client threads to create: Default is " + numClients + "\n"
                + "\tloops          The number of remote thrift calls each client makes.  Default is " + numLoops + "\n"
                + "\ttype           The type of operation to test. Options are:\n" + "\t\tread\n\t\twrite\n\t\tboth (default)\n");

        System.exit(0);
    }

    public static void main(String[] args) {

        if (args.length == 0)
            usage();

        // parse args
        for (int i = 0; i < args.length; i++) {

            if (args[i].startsWith("--")) {
                int eq = args[i].indexOf("=");

                if (eq < 0)
                    usage();

                String arg = args[i].substring(2, eq);
                String value = args[i].substring(eq + 1);

                try {
                    if (arg.equalsIgnoreCase("clients"))
                        numClients = Integer.valueOf(value);

                    if (arg.equalsIgnoreCase("loops"))
                        numLoops = Integer.valueOf(value);

                    if (arg.equalsIgnoreCase("type"))
                        type = Type.valueOf(value);
                    
                    if(arg.equalsIgnoreCase("solr"))
                        indexName = "";
                    
                    
                } catch (Throwable t) {
                    usage();
                }
            }
        }


        ExecutorService threadPool = Executors.newFixedThreadPool(numClients);
        Runnable runners[] = new Runnable[numClients];
        for (int i = 0; i < numClients; i++)
            runners[i] = getRunnable();

        System.out.println("Starting Benchmark...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numClients; i++)
            threadPool.submit(runners[i]);

        threadPool.shutdown();

        try {
            threadPool.awaitTermination(1024, TimeUnit.MINUTES);
        } catch (InterruptedException e) {

            threadPool.shutdownNow();
            System.err.println("Benchmark manually stopped");
            System.exit(1);
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Finished:");
        System.out.println("\tclients:" + numClients + ", loops:" + numLoops + ", type:" + type + ", rate(ops/sec):"
                + Math.ceil((double) ((numClients * numLoops * 1000) / (endTime - startTime))));

        System.exit(0);

    }

}
