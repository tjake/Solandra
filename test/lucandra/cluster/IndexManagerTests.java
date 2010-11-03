package lucandra.cluster;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lucandra.CassandraUtils;

import org.junit.BeforeClass;
import org.junit.Test;


public class IndexManagerTests
{
    static String indexName = String.valueOf(System.nanoTime());
    
    private class TestCassandraIndexManager extends CassandraIndexManager
    {

        
        
        public TestCassandraIndexManager(int shardsAtOnce, double collisionThreshold)
        {
            super(shardsAtOnce, collisionThreshold);
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
        
        CassandraIndexManager idx = new CassandraIndexManager(4, 0.1);
        
        Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);
        
        //Add
        for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
        {
            long id = idx.getNextId(indexName, "i"+i);
              
            assertTrue(id+" already exists "+all.size(),all.add(id));
            
            if(i % 1000 == 0)
                System.err.println(id);          
        }
        
        //Update
        for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
        {
            Long id = idx.getId(indexName, "i"+i);
            
            
            assertNotNull(id);
            
            if(i % 1000 == 0)
                System.err.println(id);          
        }
    }
    
    
    @Test
    public void testCassandraIncrement2()
    {
        final TestCassandraIndexManager idx = new TestCassandraIndexManager(4, 0.1);
        
        indexName = String.valueOf(System.nanoTime());

        ExecutorService svc = Executors.newFixedThreadPool(16);
        
        Runnable r = new Runnable() {
            
            public void run()
            {
                
                for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
                {
                    Long id = idx.getNextId(indexName, "i"+i);
                                          
                    if(i % 1000 == 0)
                        System.err.println(Thread.currentThread().getName()+" "+id);          
                }    
            }
        };
        
        for(int i=0; i<16; i++){           
            svc.submit(r);
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
