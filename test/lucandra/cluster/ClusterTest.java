package lucandra.cluster;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import lucandra.CassandraUtils;

import org.junit.BeforeClass;
import org.junit.Test;


public class ClusterTest
{
    static String indexName = String.valueOf(System.nanoTime());
    
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
        
        for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
        {
            long id = idx.incrementDocId(indexName, "i"+i);
              
            assertTrue(id+" already exists "+all.size(),all.add(id));
            
            if(i % 1000 == 0)
                System.err.println(id);          
        }      
    }
 
}
