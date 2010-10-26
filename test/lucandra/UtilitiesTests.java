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
            
            if(id < 1000)
                System.err.println(id);
        }
        
        assertEquals(CassandraUtils.maxDocsPerShard, id);
    }
    
    public void testBitSetUtil(){
        byte[] bytes = BitSetUtils.create(31);
        
        assertEquals(bytes.length, (int)Math.ceil(32/8.0));
        
        assertFalse(BitSetUtils.get(bytes, 0));
        BitSetUtils.set(bytes, 0);
        assertTrue(BitSetUtils.get(bytes, 0));
        
    }
    
    public void testVInt(){
        ByteBuffer ibytes = CassandraUtils.writeVInt(1977);
        
        assertEquals(1977, CassandraUtils.readVInt(ibytes));
    }
    
   
}
