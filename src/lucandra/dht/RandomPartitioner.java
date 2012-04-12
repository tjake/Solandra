/**
 * Copyright T Jake Luciani
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lucandra.dht;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import lucandra.CassandraUtils;

import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;

/**
 * A Random partitioner that's aware of our index hashing
 * 
 */
public class RandomPartitioner extends org.apache.cassandra.dht.RandomPartitioner
{

    public static Logger      logger         = Logger.getLogger(RandomPartitioner.class);
    private static final byte DELIMITER_BYTE = ":".getBytes()[0];

    @Override
    public org.apache.cassandra.db.DecoratedKey<BigIntegerToken> decorateKey(ByteBuffer key)
    {
        
        ByteBuffer extractedToken = extractSolandraToken(key);
               
        //non-solandra key passes through
        if(extractedToken == null)    
            return new org.apache.cassandra.db.DecoratedKey<BigIntegerToken>(super.getToken(key), key);
          
      
        //the token is pre-processed
        try
        {
            return new org.apache.cassandra.db.DecoratedKey<BigIntegerToken>(new BigIntegerToken(ByteBufferUtil.string(extractedToken)), key);
        } 
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }        
    }

    
    private ByteBuffer extractSolandraToken(ByteBuffer key)
    {
        int length = key.remaining();

        if (length > 0)
        {
            boolean found = true;
            int firstNonChar = -1;
            
            //find our delimiter
            for (int i = key.position(); i < key.limit(); i++)
            {

                if (!Character.isDigit(key.get(i)))
                {
                    if (firstNonChar < 0)
                        firstNonChar = i;                
                }
                else
                {
                    if (firstNonChar >= 0)
                    {
                        found = false;
                        break;
                    }

                    continue;
                }

                // the rest is delimiter
                if ((i - firstNonChar) >= CassandraUtils.delimeterBytes.length
                        || key.get(i) != CassandraUtils.delimeterBytes[i - firstNonChar])
                {                    
                    found = false;
                    break;
                }
                else
                {

                    // Success
                    if ((i - firstNonChar) == (CassandraUtils.delimeterBytes.length - 1))
                        break;
                }
            }
            
            if(firstNonChar < 0)
                found = false;

            if (found)
            {
                //return extracted token
                ByteBuffer extractedToken = key.duplicate();
                extractedToken.position(key.position());
                extractedToken.limit(key.position() + firstNonChar - key.position());
                
                return extractedToken;
            }           
        }

        return null;
    }
    
    public org.apache.cassandra.db.DecoratedKey<BigIntegerToken> convertFromDiskFormat(ByteBuffer fromdisk)
    {
        // find the delimiter position
        int splitPoint = -1;
        for (int i = fromdisk.position(); i < fromdisk.limit(); i++)
        {
            if (fromdisk.get(i) == DELIMITER_BYTE)
            {
                splitPoint = i;
                break;
            }
        }
        assert splitPoint != -1;
        
        ByteBuffer key = fromdisk.duplicate();
        key.position(splitPoint + 1);
        return decorateKey(key);
    }

    @Override
    public BigIntegerToken getToken(ByteBuffer key)
    {
        ByteBuffer extractedToken = extractSolandraToken(key);

        if(extractedToken == null)
            return super.getToken(key);
        
        //the token is pre-processed
        try
        {
            return new BigIntegerToken(ByteBufferUtil.string(extractedToken));
        } 
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }   
    }

}
