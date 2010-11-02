package lucandra;

import java.nio.ByteBuffer;

import junit.framework.TestCase;


public class UtilitiesTests extends TestCase {
 
    
    
    
    public void testVInt(){
        ByteBuffer ibytes = CassandraUtils.writeVInt(1977);
        
        assertEquals(1977, CassandraUtils.readVInt(ibytes));
    }
    
}
