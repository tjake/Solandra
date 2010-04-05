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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WikipediaImporter {

    private ExecutorService threadPool;
    private Queue<Future<Integer>> resultSet;
    private int pageCount;
    private int loadCount;
    private long size;
    private long startTime;
    private long lastTime;

    public WikipediaImporter() {
        threadPool = Executors.newFixedThreadPool(64);
        resultSet = new LinkedBlockingQueue<Future<Integer>>();
        pageCount = 0;
        loadCount = 0;
        size = 0;

        startTime = System.currentTimeMillis();
        lastTime = System.currentTimeMillis();
    }

    private static void usage() {
        System.err.println("WikipediaImporter file.xml");
        System.exit(0);
    }

    private void readFile(String fileName) throws IOException {

        InputStream inputFile = new FileInputStream(fileName);
        BufferedReader fileStream = new BufferedReader(new InputStreamReader(inputFile));

        // rather than xml parse, just do something fast & simple.
        String line;
        Article page = new Article();
        boolean inText = false;

        while ((line = fileStream.readLine()) != null) {

            // Page
            if (line.contains("<doc>")) {
                page = new Article();
                continue;
            }

            if (line.contains("</doc>")) {

                if (++pageCount % 5000 == 0) {
                    Future<Integer> result;

                    while ((result = resultSet.poll()) != null) {
                        try {
                            size += result.get();
                            loadCount++;
                            long now = System.currentTimeMillis();
                            if ((now - lastTime) / 1000.0 > 1) {
                                System.err.println("Loaded (" + loadCount + ") " + size / 1000.0 + "Kb, in " + (now - startTime) / 1000.0 + ", avg "
                                        + (loadCount / ((now - startTime) / 1000)) + " docs/sec");
                                lastTime = now;
                            }
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }

                }

                indexPage(page); // index each page
            }

            // title
            if (line.contains("<title>")) {
                page.title = line.substring(line.indexOf("<title>") + 7, line.indexOf("</title>"));

                continue;
            }

            // url
            if (line.contains("<url>")) {
                if (page.url == null)
                    page.url = line.substring(line.indexOf("<url>") + 5, line.indexOf("</url>"));

                continue;
            }

            // article text
            if (line.contains("<abstract>")) {

                if (line.contains("</abstract>")) {
                    page.text = line.substring(line.indexOf("<abstract>") + 10, line.indexOf("</abstract>")).getBytes();
                } else {
                    page.text = line.substring(line.indexOf("<abstract>" + 10)).getBytes();
                    inText = true;
                    continue;
                }
            }

            if (inText) {

                String text = line;
                if (line.contains("</abstract>"))
                    text = line.substring(0, line.indexOf("</abstract>"));

                byte[] newText = new byte[page.text.length + text.getBytes().length];

                System.arraycopy(page.text, 0, newText, 0, page.text.length);
                System.arraycopy(text.getBytes(), 0, newText, page.text.length, text.getBytes().length);

                page.text = newText;
            }

            if (line.contains("</abstract>")) {
                inText = false;
                continue;
            }
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(90, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {

        }

        Future<Integer> result;
        int size = 0;
        while ((result = resultSet.poll()) != null) {
            try {
                size += result.get();

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        long now = System.currentTimeMillis();
        System.err.println("Loaded (" + pageCount + ") " + size / 1000 + "Kb, in " + (now - lastTime) / 1000.0);

        System.err.println("done");

    }

    public void indexPage(Article page) {

        Future<Integer> result = threadPool.submit(new WikipediaIndexWorker(page));
        resultSet.add(result);

    }

    public static void main(String[] args) {

        try {

            if (args.length > 0)
                new WikipediaImporter().readFile(args[0]);
            else
                WikipediaImporter.usage();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
