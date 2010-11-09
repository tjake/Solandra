package lucandra.cluster;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lucandra.CassandraUtils;

import org.junit.BeforeClass;
import org.junit.Test;


public class IndexManagerTests
{
    static String indexName = String.valueOf(System.nanoTime());
    
    private class TestCassandraIndexManager extends CassandraIndexManager
    {

        
        
        public TestCassandraIndexManager(int shardsAtOnce)
        {
            super(shardsAtOnce);
            // TODO Auto-generated constructor stub
        }
        
        public String getToken()
        {
            return Thread.currentThread().getName();
        }
        
    }
    
    @BeforeClass
    public static void setUpBeforeClass() {       
            // start cassandra
            CassandraUtils.startup();
    }
    
    @Test
    public void testCassandraIncrement()
    {
        
        CassandraIndexManager idx = new CassandraIndexManager(4);
        
        Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);
        
        long startTime = System.currentTimeMillis();
        
        //Add
        for(int i=0; i<CassandraUtils.maxDocsPerShard*2; i++)
        {
            long id = idx.getNextId(indexName, "i"+i);
              
            assertTrue(id+" already exists "+all.size(),all.add(id));
            
            if(i % 10000 == 0){
                long endTime = System.currentTimeMillis();
                System.err.println("added:"+id+", 10k iterations in "+(endTime - startTime)/1000+" sec");
                startTime = endTime;         
            }
        }
        
        //Update
        for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
        {
            Long id = idx.getId(indexName, "i"+i);
            
            
            assertNotNull(id);
            
            if(i % 10000 == 0){
                long endTime = System.currentTimeMillis();
                System.err.println("updated:"+id+", 10k iterations in "+(endTime - startTime)/1000+" sec");
                startTime = endTime;      
            }

        }
    }
    
    
    @Test
    public void testCassandraIncrement2()
    {
        
        indexName = String.valueOf(System.nanoTime());

        ExecutorService svc = Executors.newFixedThreadPool(16);
        
        
        
        List<Callable<Set<Long>>> callables = new ArrayList<Callable<Set<Long>>>();
        for(int i=0; i<16; i++){
            Callable<Set<Long>> r = new Callable<Set<Long>>() {
                
                public Set<Long> call()
                {
                    final TestCassandraIndexManager idx = new TestCassandraIndexManager(4);

                   long startTime = System.currentTimeMillis();
                    
                    Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);

                    for(int i=0; i<CassandraUtils.maxDocsPerShard/10; i++)
                    {
                        Long id = idx.getNextId(indexName, "i"+i);
                        assertTrue(id+" already exists "+all.size(),all.add(id));
                        
                        if(i % 10000 == 0){
                            long endTime = System.currentTimeMillis();
                            System.err.println(Thread.currentThread().getName()+" id:"+id+", 10k iterations in "+(endTime - startTime)/1000+" sec");
                            startTime = endTime;      
                        }
                    }
                    
                    return all;
                }

             
            };
            
            callables.add(r);
        }
        
        try
        {
            List<Future<Set<Long>>> results = svc.invokeAll(callables);
            
            Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);
            
            for(Future<Set<Long>> result : results)
            {
                Set<Long> thread = result.get();
                
                for(Long id : thread) {
                    if(!all.add(id)){
                        System.err.println(id+" already exists "+all.size());
                    }
                }
            }            
        }
        catch (InterruptedException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        catch (ExecutionException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        svc.shutdown();
        
        try
        {
            svc.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    
 
}
