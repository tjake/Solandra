package lucandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.service.Cassandra;
import org.apache.lucene.index.Term;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CassandraUtils {
    
    public static final String keySpace  = "Lucandra";
    public static final String termColumn = "Terms";
    public static final String docColumn  = "Documents";

    public static final String delimeter = "|x|";
    
    public static Cassandra.Client createConnection() throws TTransportException {
      // temporarily connect to cassandra
      TSocket socket = new TSocket("localhost", 9160);
      TTransport trans = new TFramedTransport(socket);
      trans.open();
      TProtocol protocol = new TBinaryProtocol(trans);

      return new Cassandra.Client(protocol);
    }
    
    public static String createColumnName(Term term){
        return createColumnName(term.field(), term.text());
    }
    
    public static String createColumnName(String field, String text){
        return field+delimeter+text;
    }
    
    public static Term parseTerm(byte[] termStr){
        String[] parts = null;
        
        try{
            parts = new String(termStr,"UTF-8").split("\\|x\\|");
        }catch(UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
        
        if(parts == null || parts.length != 2){
            throw new RuntimeException("invalid term format: "+termStr);
        }
        
        return new Term(parts[0],parts[1]);
    }
    
    public static final byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
    }

    public static final int byteArrayToInt(byte [] b) {
        return (b[0] << 24)
                + ((b[1] & 0xFF) << 16)
                + ((b[2] & 0xFF) << 8)
                + (b[3] & 0xFF);
    }
    
    public static final byte[] randomUUID(){
      
        ByteBuffer buffer = ByteBuffer.allocate(16);
        UUID docUUID = UUID.randomUUID();
        buffer.putLong(docUUID.getMostSignificantBits());
        buffer.putLong(docUUID.getLeastSignificantBits());
        return buffer.array();        
    }
}
