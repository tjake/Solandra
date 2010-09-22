package lucandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;


public class UtilitiesTests extends TestCase {

    
    static{
    }

    
    /*public void testIndexManager(){
        String indexName = String.valueOf(System.nanoTime());
        int database = 11;
        
        ConnectionSpec connectionSpec = DefaultConnectionSpec.newSpec("localhost", 6379, database, "jredis".getBytes());
        int connCnt = 7;
     
        // create the service -- well this is it as far as usage goes:  set the number of connections for the service pool
        // You can use this anywhere you would use JRedis instances and it is thread safe.
        // 
        JRedisService service = new JRedisService(connectionSpec, connCnt);
        
        
        AbstractIndexManager docCounter = new RedisIndexManager(service);
       
        int id = 0;
        for(int i=0; i<1000000; i++){
            id = docCounter.incrementDocId(indexName);
            if(i % 100 == 0)
                System.err.println(id);
        }
        
        assertEquals(1000000, id);
    }
    
    public void testBitSetUtil(){
        byte[] bytes = BitSetUtils.create(31);
        
        assertEquals(bytes.length, (int)Math.ceil(32/8.0));
        
        assertFalse(BitSetUtils.get(bytes, 0));
        BitSetUtils.set(bytes, 0);
        assertTrue(BitSetUtils.get(bytes, 0));
        
    }
    
    public void testVInt(){
        byte[] ibytes = CassandraUtils.writeVInt(1977);
        
        assertEquals(1977, CassandraUtils.readVInt(ibytes));
    }
    */
    public void testBitSetReconciler() throws UnavailableException, TimeoutException, IOException, InvalidRequestException{
                     
        CassandraUtils.startup();

        
        int size = 1000; //CassandraUtils.maxDocsPerShard;
       
        byte[] key = "index1".getBytes();
        byte[] col = "term1".getBytes();
        
        List<RowMutation> rlist = new ArrayList<RowMutation>();
        
        
        for(int i=0; i<size/2; i++){
            byte[] bytes1 = BitSetUtils.create(size);          
            
            BitSetUtils.set(bytes1, i);
            
            
            RowMutation rm1 = new RowMutation(CassandraUtils.keySpace,key);
            rm1.add(new QueryPath("MI",null, col), bytes1, new TimestampClock(System.currentTimeMillis()));
            
            rlist.add(rm1);
            
        }      
                  
      
        
       
        StorageProxy.mutateBlocking(rlist,ConsistencyLevel.ONE);
        
        ////////Second half
        byte[] bytes2 = BitSetUtils.create(size);
        
        for(int i=size/2; i<size; i++){
            BitSetUtils.set(bytes2,i);
        }
        
        RowMutation rm2 = new RowMutation(CassandraUtils.keySpace,key);
       
        rm2.add(new QueryPath("MI",null, col), bytes2, new TimestampClock(System.currentTimeMillis()));
        StorageProxy.mutateBlocking(Arrays.asList(rm2),ConsistencyLevel.ALL);
     
        ColumnParent columnParent = new ColumnParent("MI");
        //Check for merged version
        List<Row> rows = StorageProxy.readProtocol(Arrays.asList((ReadCommand)new SliceFromReadCommand(CassandraUtils.keySpace, key, columnParent, new byte[] {}, new byte[]{},
                       false, Integer.MAX_VALUE)), ConsistencyLevel.ONE);
        
        
        assertEquals(1,rows.size());
        
        Row row = rows.get(0);
        
        byte[] bytes3 = row.cf.getColumn(col).value();
       
         
        for(int i=0; i<size; i++)
            assertTrue(BitSetUtils.get(bytes3, i));
         
        
    }
    
   
}
