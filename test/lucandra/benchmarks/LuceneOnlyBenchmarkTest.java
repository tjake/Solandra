package lucandra.benchmarks;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

public class LuceneOnlyBenchmarkTest {

    private enum Type {
        read, write, both
    }

    private static int numClients = 20;
    private static int numLoops = 10;
    private static Type type = Type.both;
    private static String indexName = "/tmp/bench";
    private static String text = "this is a benchmark of lucandra";
    private static String queryString = "text:benchmark";
    private static Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
    private static int threadId = 0;
    private static  Query query;
    private static final Document doc;
    private static  IndexWriter indexWriter;

    static {

        try { 
            query = new QueryParser(Version.LUCENE_CURRENT, "text", analyzer).parse(queryString);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        
    
       
        doc = new Document();
        doc.add(new Field("text", text, Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS ));

    }

    private static Runnable getRunnable() throws IOException {

        return new Runnable() {

            private IndexReader indexReader  = IndexReader.open(indexName); 
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

                        long start = System.currentTimeMillis();
                        TopDocs td = new IndexSearcher(indexReader).search(query, 10);

                        total = td.totalHits;

                        indexReader = indexReader.reopen();

                        long end = System.currentTimeMillis();
                        //if (i % 1000 == 999)
                        System.err.println("Thread " + myThreadId + ": total " + total+" in "+(end - start)+"ms");
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
                    
                        if(i % 1000 == 0)
                            indexWriter.commit();

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
                            TopDocs td = new IndexSearcher(indexReader).search(query, 10);
                            total = td.totalHits;

                            indexReader = indexReader.reopen();

                            if (i % 999 == 0)
                                System.err.println("Thread " + myThreadId + ": total " + total);

                        } else {
                            indexWriter.addDocument(doc, analyzer);
                        }
                        
                        if (i % 1000 == 0)
                            indexWriter.commit();
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

        System.err.print(LuceneOnlyBenchmarkTest.class.getSimpleName() + " [--clients=<client-count>] [--loops=<loop-count>] [--type=<test-type>]\n"
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
        
        try {
            if(type == Type.both || type == Type.write)
                indexWriter = new IndexWriter(indexName, analyzer,true);
            else
                indexWriter = new IndexWriter(indexName, analyzer,false);
        } catch (CorruptIndexException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (LockObtainFailedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
            
        

        ExecutorService threadPool = Executors.newFixedThreadPool(numClients);
        Runnable runners[] = new Runnable[numClients];
        for (int i = 0; i < numClients; i++)
            try {
                runners[i] = getRunnable();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(100);
            }

        
        System.out.println("Starting Benchmark...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numClients; i++)
            threadPool.submit(runners[i]);

        threadPool.shutdown();

        try {
            threadPool.awaitTermination(1024, TimeUnit.MINUTES);
            if(type == Type.both || type == Type.write){
                indexWriter.commit();
                indexWriter.close();
            }
        } catch (InterruptedException e) {

            threadPool.shutdownNow();
            System.err.println("Benchmark manually stopped");
            System.exit(1);
        } catch (CorruptIndexException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        
        
        long endTime = System.currentTimeMillis();

        System.out.println("Finished:");
        System.out.println("\tclients:" + numClients + ", loops:" + numLoops + ", type:" + type + ", rate(ops/sec):"
                + Math.ceil((double) ((numClients * numLoops * 1000) / (endTime - startTime))));

        
        System.exit(0);
        
        
    }

}
