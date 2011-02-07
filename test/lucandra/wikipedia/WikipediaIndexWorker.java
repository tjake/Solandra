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
package lucandra.wikipedia;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class WikipediaIndexWorker implements Callable<Integer> {

    // each worker thread has a connection to cassandra
    private static ConcurrentLinkedQueue<List<SolrInputDocument>> allDocBuffers = new ConcurrentLinkedQueue<List<SolrInputDocument>>();
    private static ThreadLocal<CommonsHttpSolrServer> clientPool = new ThreadLocal<CommonsHttpSolrServer>();
    private static ThreadLocal<List<SolrInputDocument>> docBuffer = new ThreadLocal<List<SolrInputDocument>>();
    private static CommonsHttpSolrServer oneClient;
    public  static final ArrayList<String> hosts = new ArrayList<String>();
    private static final Random r = new Random();
    
    static int port = 8983;
    
    //Add shutdown hook for batched commits to complete
    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                List<SolrInputDocument> docs;
                while ((docs = allDocBuffers.poll()) != null) {
                    try {
                       
                        if(!docs.isEmpty())
                            oneClient.add(docs);
                            
                    } catch (SolrServerException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                System.err.println("committed");
            }
        });
    }
    
    // this is the article to index
    private Article article;

    public WikipediaIndexWorker(Article article) {
        this.article = article;
    }

    private CommonsHttpSolrServer getIndexWriter() throws MalformedURLException  {
        CommonsHttpSolrServer indexWriter = clientPool.get();

        if (indexWriter == null) {
            
            if(hosts.size() == 0)
                throw new RuntimeException("no hosts defined");   
            
            indexWriter = new StreamingUpdateSolrServer("http://"+hosts.get(r.nextInt(hosts.size()))+":" + port + "/solandra/wikassandra", 512, 64/hosts.size());

            clientPool.set(indexWriter);
        }

        return indexWriter;
    }

    public Integer call() throws Exception {

        CommonsHttpSolrServer indexWriter = getIndexWriter();

        SolrInputDocument doc = new SolrInputDocument();
        
        doc.addField("title", article.title);
        
        if(article.text != null)
            doc.addField("text", new String(article.text,"UTF-8"));
        
        doc.addField("url", article.url);
        
        indexWriter.add(doc);
         
        return article.getSize();
    }

}
