/**
 * Copyright 2010 T Jake Luciani
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

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import lucandra.CassandraUtils;
import lucandra.IndexWriter;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.apache.thrift.transport.TTransportException;

public class WikipediaIndexWorker implements Callable<Integer> {

    // each worker thread has a connection to cassandra
    private static ConcurrentLinkedQueue<lucandra.IndexWriter> allClients = new ConcurrentLinkedQueue<IndexWriter>();
    private static ThreadLocal<lucandra.IndexWriter> clientPool = new ThreadLocal<lucandra.IndexWriter>();
    private static ThreadLocal<Integer> batchCount = new ThreadLocal<Integer>();

    // get ring info
    private static List<TokenRange> ring;
    static {
        try {
            Cassandra.Iface client = CassandraUtils.createConnection();
            ring = client.describe_ring(CassandraUtils.keySpace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //Add shutdown hook for batched commits to complete
    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                lucandra.IndexWriter w;
                while ((w = allClients.poll()) != null) {
                    w.commit();
                }

                System.err.println("committed");
            }
        });
    }
    
    // this is shared by all workers
    private static Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);

    // this is the article to index
    private Article article;

    public WikipediaIndexWorker(Article article) {
        this.article = article;
    }

    private lucandra.IndexWriter getIndexWriter() throws TTransportException {
        lucandra.IndexWriter indexWriter = clientPool.get();

        if (indexWriter == null) {

            Random r = new Random();
            List<String> endpoints = ring.get(r.nextInt(ring.size())).endpoints;
            String endpoint = endpoints.get(r.nextInt(endpoints.size()));

            indexWriter = new lucandra.IndexWriter("wikipedia", CassandraUtils.createConnection(endpoint, 9160, false));
            clientPool.set(indexWriter);

            indexWriter.setAutoCommit(false);

            batchCount.set(0);
        }

        return indexWriter;
    }

    public Integer call() throws Exception {

        lucandra.IndexWriter indexWriter = getIndexWriter();

        Document d = new Document();

        d.add(new Field("title", article.title, Store.YES, Index.ANALYZED));

        if (article.text != null)
            d.add(new Field("text", new String(article.text), Store.YES, Index.ANALYZED));

        d.add(new Field("url", article.url, Store.YES, Index.NOT_ANALYZED));

        indexWriter.addDocument(d, analyzer);

        Integer c = batchCount.get();
        if ((c + 1) % 64 == 0) {
            indexWriter.commit();
        }

        batchCount.set(c + 1);

        return article.getSize();
    }

}
