package lucandra.benchmarks;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lucandra.CassandraUtils;
import lucandra.IndexReader;
import lucandra.IndexWriter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

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
    private static Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
    private static int threadId = 0;
    private static final Query query;
    private static final Document doc;

    static {

        try {
            query = new QueryParser(Version.LUCENE_CURRENT, "text", analyzer).parse(queryString);
        } catch (ParseException e) {
           throw new RuntimeException(e);
        }
        doc = new Document();
        doc.add(new Field("text", text, Store.YES, Index.ANALYZED_NO_NORMS, TermVector.NO ));

    }

    private static Runnable getRunnable() {

        return new Runnable() {

            private final IndexReader indexReader = new IndexReader(indexName);
            private final IndexWriter indexWriter = new IndexWriter(indexName);
            private final IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            private final int myThreadId = threadId++;

            public void run() {

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

            }

            private void read() {

                int total = 0;

                try {
                    for (int i = 0; i < numLoops; i++) {

                        TopDocs td = indexSearcher.search(query, 10);

                        total = td.totalHits;

                        indexReader.reopen();

                        //if (i % 1000 == 999)
                            System.err.println("Thread " + myThreadId + ": total " + total);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }catch(Throwable t){
                    t.printStackTrace();
                }

                if (myThreadId == 0)
                    System.err.println("Documents found: " + total);
            }

            private void write() {

                for (int i = 0; i < numLoops; i++) {
                    try {
                        indexWriter.addDocument(doc, analyzer);
                    } catch (CorruptIndexException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            private void both() {

                int total = 0;

                for (int i = 0; i < numLoops; i++) {
                    try {
                        if (i % 2 == 1) {
                            TopDocs td = indexSearcher.search(query, 10);
                            total = td.totalHits;

                            indexReader.reopen();

                            if (i % 1000 == 999)
                                System.err.println("Thread " + myThreadId + ": total " + total);

                        } else {
                            indexWriter.addDocument(doc, analyzer);
                        }
                    } catch (CorruptIndexException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (myThreadId == 0)
                    System.err.println("Documents found: " + total);
            }
        };
    }

    private static void usage() {

        System.err.print(BenchmarkTest.class.getSimpleName() + " [--clients=<client-count>] [--loop=<loop-count>] [--type=<test-type>]\n"
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
                } catch (Throwable t) {
                    usage();
                }
            }
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(numClients);
        Runnable runners[] = new Runnable[numClients];
        for (int i = 0; i < numClients; i++)
            runners[i] = getRunnable();

        CassandraUtils.startup();
        
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
