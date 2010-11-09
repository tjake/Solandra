package lucandra.dht;

import static com.google.common.base.Charsets.UTF_8;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lucandra.CassandraUtils;

import org.apache.cassandra.dht.BigIntegerToken;

import org.apache.log4j.Logger;

/**
 * A Random partitioner that's aware of our index hashing
 * 
 */
public class RandomPartitioner extends org.apache.cassandra.dht.RandomPartitioner
{

    public static Logger logger = Logger.getLogger(RandomPartitioner.class);

    private static final byte DELIMITER_BYTE = ":".getBytes()[0];

    public DecoratedKey<BigIntegerToken> decorateKey(ByteBuffer key)
    {
        return new DecoratedKey<BigIntegerToken>(getToken(key), key);
    }
    
    public DecoratedKey<BigIntegerToken> convertFromDiskFormat(ByteBuffer fromdisk)
    {
        // find the delimiter position
        int splitPoint = -1;
        for (int i = fromdisk.position()+fromdisk.arrayOffset(); i < fromdisk.limit()+fromdisk.arrayOffset(); i++)
        {
            if (fromdisk.array()[i] == DELIMITER_BYTE)
            {
                splitPoint = i;
                break;
            }
        }
        assert splitPoint != -1;

        // and decode the token and key
        String token = new String(fromdisk.array(), fromdisk.position()+fromdisk.arrayOffset(), splitPoint, UTF_8);
        byte[] key = Arrays.copyOfRange(fromdisk.array(), splitPoint + 1, fromdisk.limit()+fromdisk.arrayOffset());
        return new DecoratedKey<BigIntegerToken>(new BigIntegerToken(token), ByteBuffer.wrap(key));
    }
   
    public BigIntegerToken getToken(ByteBuffer key)
    {
        int minLength = CassandraUtils.keySigBytes + CassandraUtils.delimeterBytes.length;
        
        if (key.remaining() >= minLength)
        {
            // check for our delimiter
            boolean found = true;
            for (int i = 0; i < minLength; i++)
            {
                // first n chars are digits
                if (i < CassandraUtils.keySigBytes)
                {
                    if (!Character.isDigit(key.get(i)))
                    {
                        found = false;
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }

                // the rest is delimiter
                if (key.get(i) != CassandraUtils.delimeterBytes[i - CassandraUtils.keySigBytes])
                {
                    found = false;
                    break;
                }
            }

            if (found)
            {
                String tokStr = new String(key.array(), key.position() + key.arrayOffset(),CassandraUtils.keySigBytes, CassandraUtils.UTF_8);
               // logger.debug("Token hijacked:"+tokStr);
                               
                return new BigIntegerToken(tokStr);
            }
        }

        return super.getToken(key);
    }

}
