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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

public class LuceneOnlyWikipediaImporter {

    private int pageCount;
    private long size;
    private long startTime;
    private long lastTime;
    private Analyzer analyzer;
    private IndexWriter indexWriter;

    public LuceneOnlyWikipediaImporter() {
        pageCount = 0;
        size = 0;

        analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
        try {
            indexWriter = new IndexWriter(FSDirectory.open(new File("/tmp/wikassandra")), analyzer, true, MaxFieldLength.UNLIMITED);
        } catch (CorruptIndexException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        startTime = System.currentTimeMillis();
        lastTime = System.currentTimeMillis();
    }

    private static void usage() {
        System.err.println("LuceneOnlyWikipediaImporter file.xml");
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

                    long now = System.currentTimeMillis();
                    if ((now - lastTime) / 1000.0 > 1) {
                        System.err.println("Loaded (" + pageCount + ") " + size / 1000.0 + "Kb, in " + (now - startTime) / 1000.0 + ", avg "
                                + (pageCount / ((now - startTime) / 1000)) + " docs/sec");
                        lastTime = now;
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

        indexWriter.commit();
        indexWriter.close();

        long now = System.currentTimeMillis();
        System.err.println("Loaded (" + pageCount + ") " + size / 1000 + "Kb, in " + (now - lastTime) / 1000.0);

        System.err.println("done");

    }

    public void indexPage(Article article) throws CorruptIndexException, IOException {

        Document d = new Document();

        d.add(new Field("title", article.title, Store.YES, Index.ANALYZED));

        if (article.text != null)
            d.add(new Field("text", new String(article.text), Store.YES, Index.ANALYZED));

        d.add(new Field("url", article.url, Store.YES, Index.NOT_ANALYZED));

        indexWriter.addDocument(d, analyzer);

    }

    public static void main(String[] args) {

        try {

            if (args.length > 0)
                new LuceneOnlyWikipediaImporter().readFile(args[0]);
            else
                LuceneOnlyWikipediaImporter.usage();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
