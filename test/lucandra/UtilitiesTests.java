package lucandra;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import junit.framework.TestCase;
import lucandra.cluster.AbstractIndexManager;
import lucandra.cluster.JVMIndexManager;


public class UtilitiesTests extends TestCase {
 
    public void testRandomToken() throws Exception{
        
        for(int i=0; i<100; i++){
            ByteBuffer bb = CassandraUtils.hashKeyBytes(("index"+i).getBytes(),CassandraUtils.delimeterBytes,"foo".getBytes());
            System.err.println(ByteBufferUtil.string(bb));
        }
        
    }

    
    public void testIndexManager(){
        String indexName = String.valueOf(System.nanoTime());
        int shardsAtOnce = 4;       
        
        AbstractIndexManager docCounter = new JVMIndexManager(shardsAtOnce);
       
               
        long id = 0;
        for(int i=0; i<CassandraUtils.maxDocsPerShard*shardsAtOnce+1; i++){
            id = docCounter.incrementDocId(indexName);
            
            if(i < 1000)
                System.err.println(id);
        }
        
        assertEquals(CassandraUtils.maxDocsPerShard, id);
    }
    
    
    public void testVInt(){
        ByteBuffer ibytes = CassandraUtils.writeVInt(1977);
        
        assertEquals(1977, CassandraUtils.readVInt(ibytes));
    }
    
}
