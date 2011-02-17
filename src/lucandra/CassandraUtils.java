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
package lucandra;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;

public class CassandraUtils
{

    public static final Properties           properties;
    static
    {      
        try
        {
            
            properties = new Properties();
            properties.load(CassandraUtils.class.getClassLoader().getResourceAsStream("solandra.properties"));
        
        } 
        catch (FileNotFoundException e)
        {
            throw new RuntimeException("Can't locate solandra.properties file");
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error reading solandra.properties file");
        }
    }
    
    
    public static final String               keySpace               = properties.getProperty("solandra.keyspace", "L");
    public static final String               termVecColumnFamily    = "TI";
    public static final String               docColumnFamily        = "Docs";
    public static final String               metaInfoColumnFamily   = "TL";
    public static final String               schemaInfoColumnFamily = "SI";

    public static final String               positionVectorKey      = "P";
    public static final String               offsetVectorKey        = "O";
    public static final String               termFrequencyKey       = "F";
    public static final String               normsKey               = "N";
    
    public static final String               schemaKey              = "S";
    public static final String               cachedCol              = "CC";
    
    public static final ByteBuffer           cachedColBytes         = ByteBufferUtil.bytes(cachedCol);
    public static final ByteBuffer           positionVectorKeyBytes = ByteBufferUtil.bytes(positionVectorKey);
    public static final ByteBuffer           offsetVectorKeyBytes   = ByteBufferUtil.bytes(offsetVectorKey);
    public static final ByteBuffer           termFrequencyKeyBytes  = ByteBufferUtil.bytes(termFrequencyKey);
    public static final ByteBuffer           normsKeyBytes          = ByteBufferUtil.bytes(normsKey);

    public static final ByteBuffer           schemaKeyBytes         = ByteBufferUtil.bytes(schemaKey);
    
    public static final List<Number>         emptyArray             = Arrays.asList(new Number[] {});
    public static final String               delimeter              = new String("\uffff");
    public static final byte[]               delimeterBytes         = ByteBufferUtil.bytes(delimeter).array();

    public static final String               finalToken             = new String("\ufffe\ufffe");
    public static final ByteBuffer           finalTokenBytes        = ByteBufferUtil.bytes(finalToken);

    public static final String               documentMetaField      = delimeter + "META" + delimeter;
    public static final ByteBuffer           documentMetaFieldBytes = ByteBufferUtil.bytes(documentMetaField);

    public static final boolean              indexHashingEnabled    = Boolean.valueOf(System.getProperty(
            "index.hashing", "true"));
    
    public static  int                       retryAttempts             = Integer.valueOf(properties.getProperty("cassandra.retries", "1024"));
    public static  int                       retryAttemptSleep         = Integer.valueOf(properties.getProperty("cassandra.retries.sleep", "100")); 
    
    //how often to check for cache invalidation
    public static int                        cacheInvalidationInterval = Integer.valueOf(properties.getProperty("solandra.cache.invalidation.check.interval", "1000"));
  
    public static final ConsistencyLevel     consistency               = ConsistencyLevel.valueOf(properties.getProperty("solandra.consistency", ConsistencyLevel.ONE.name()));
    
    
    public static final QueryPath            metaColumnPath            = new QueryPath(CassandraUtils.docColumnFamily);

    public static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final Logger              logger                 = Logger.getLogger(CassandraUtils.class);

    private static boolean                   cassandraStarted       = false;

    
    public static synchronized void setStartup(){
    	if(cassandraStarted){
    		throw new RuntimeException("You attempted to set the casandra started flag after it has started");
    	}
    	
    	cassandraStarted = true;
    }
    
    // Start Cassandra up!!!
    public static synchronized void startup()
    {

        if (cassandraStarted)
            return;

        cassandraStarted = true;

        
        System.setProperty("cassandra-foreground", "1");
        
        final CassandraDaemon daemon = new CassandraDaemon();
        
        try
        {
            //run in own thread
            new Thread(new Runnable() {
                
                public void run()
                {
                    daemon.activate();                   
                }
            }).start();
        }
        catch (Throwable e)
        {

            e.printStackTrace();
            System.exit(2);
        }

        //wait for startup to complete
        try
        {
            daemon.getStartedLatch().await(1, TimeUnit.HOURS);
        }
        catch (InterruptedException e1)
        {
            logger.error("Cassandra not started after 1 hour");
            System.exit(3);
        }       
    }

    public static ByteBuffer createColumnName(Fieldable field)
    {
        return ByteBuffer.wrap(createColumnName(field.name(), field.stringValue()));
    }
    
    public static ByteBuffer createColumnName(Term term)
    {
        return ByteBuffer.wrap(createColumnName(term.field(), term.text()));
    }

    public static byte[] createColumnName(String field, String text)
    {

        // case of all terms
        if (field.equals("") || text == null)
            return delimeterBytes;

        try
        {
            return (field + delimeter + text).getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("JVM doesn't support UTF-8", e);
        }
    }

    
    
    public static Term parseTerm(String termStr)
    {

        int index = termStr.indexOf(delimeter);

        if (index < 0)
        {
            throw new RuntimeException("invalid term format: " + index + " " + termStr);
        }

        return new Term(termStr.substring(0, index), termStr.substring(index + delimeter.length()));
    }

    public static final byte[] intToByteArray(int value)
    {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public static final int byteArrayToInt(ByteBuffer b)
    {
        return (b.get(b.position() + 0) << 24)
                + ((b.get(b.position() + 1) & 0xFF) << 16)
                + ((b.get(b.position() + 2) & 0xFF) << 8)
                + (b.get(b.position() + 3) & 0xFF);
    }

    public static final ByteBuffer intVectorToByteArray(List<Number> intVector)
    {

        if (intVector.size() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        if (intVector.get(0) instanceof Byte)
            return ByteBuffer.wrap(new byte[] { intVector.get(0).byteValue() });

        
       
        ByteBuffer buffer = ByteBuffer.allocate(4 * (intVector.size()+1));

        //Number of int's
        buffer.putInt(intVector.size());
        
        for (Number i : intVector)
        {
            buffer.putInt(i.intValue());
        }
        buffer.flip();
        return buffer;
    }


    public static final int[] byteArrayToIntArray(ByteBuffer b)
    {

        if (b.remaining() % 4 != 0)
            throw new RuntimeException("Not a valid int array:" + b.remaining());

        int[] intArray = new int[b.remaining() / 4];
        int idx = 0;

        for (int i = b.position(); i < b.limit(); i += 4)
        {
            intArray[idx++] = (b.get(i) << 24) + ((b.get(i + 1) & 0xFF) << 16)
                    + ((b.get(i + 2) & 0xFF) << 8) + (b.get(i + 3) & 0xFF);
        }

        return intArray;
    }

   

 

    public static void addMutations(Map<ByteBuffer, RowMutation> mutationList, String columnFamily, byte[] column,
            ByteBuffer key, byte[] value)
    {
        addMutations(mutationList, columnFamily, ByteBuffer.wrap(column), key, ByteBuffer.wrap(value));
    }

    public static void addMutations(Map<ByteBuffer, RowMutation> mutationList, String columnFamily, byte[] column,
            ByteBuffer key, ByteBuffer value)
    {
        addMutations(mutationList, columnFamily, ByteBuffer.wrap(column), key, value);
    }

    public static void addMutations(Map<ByteBuffer, RowMutation> mutationList, String columnFamily, ByteBuffer column,
            ByteBuffer key, ByteBuffer value)
    {

        // Find or create row mutation
        RowMutation rm = mutationList.get(key);
        if (rm == null)
        {
            rm = new RowMutation(CassandraUtils.keySpace, key);
            mutationList.put(key, rm);
        }

        if (value == null)
        { // remove

            if (column != null)
            {
                rm.delete(new QueryPath(columnFamily, null, column), System.nanoTime());
            }
            else
            {
                rm.delete(new QueryPath(columnFamily), System.nanoTime());
            }

        }
        else
        { // insert

            rm.add(new QueryPath(columnFamily, null, column), value, System.nanoTime());
    
        }
    }

    public static void robustInsert(ConsistencyLevel cl, RowMutation... mutations )
    {

        int attempts = 0;
        while (attempts++ < retryAttempts)
        {

            try
            {
                StorageProxy.mutate(Arrays.asList(mutations), cl);
                return;
            }
            catch (UnavailableException e)
            {

            }
            catch (TimeoutException e)
            {

            }
     

            try
            {
                Thread.sleep(retryAttemptSleep);
            }
            catch (InterruptedException e)
            {

            }
        }

        throw new RuntimeException("insert failed after 10 attempts");
    }

    public static List<Row> robustRead(ConsistencyLevel cl, ReadCommand... rc) throws IOException
    {      
        List<Row> rows = null;
        int attempts = 0;
        while (attempts++ < retryAttempts)
        {
            try
            {
                rows = StorageProxy.read(Arrays.asList(rc), cl);
                return rows;
            }
            catch (UnavailableException e1)
            {

            }
            catch (TimeoutException e1)
            {

            }
            catch (InvalidRequestException e)
            {
                throw new IOException(e);
            }

            try
            {
                Thread.sleep(retryAttemptSleep);
            }
            catch (InterruptedException e)
            {

            }
        }

        throw new IOException("Read command failed after "+retryAttempts+"attempts");
    }

    public static List<Row> robustRead(ByteBuffer key, QueryPath qp, List<ByteBuffer> columns, ConsistencyLevel cl) throws IOException
    {

        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, key, qp, columns);

        return robustRead(cl,rc);

    }

    /** Read the object from bytes string. */
    public static Object fromBytes(ByteBuffer data) throws IOException, ClassNotFoundException
    {

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(ByteBufferUtil.getArray(data)));
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    /** Write the object to bytes. */
    public static ByteBuffer toBytes(Object o) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return ByteBuffer.wrap(baos.toByteArray());
    }

    public static BigInteger md5hash(ByteBuffer data)
    {
        byte[] result = FBUtilities.hash(data);
        BigInteger hash = new BigInteger(result);
        return hash.abs();        
    }
    
    public static ByteBuffer hashBytes(byte[] key)
    {      
       
            byte[] hashBytes = null;
            try
            {
                hashBytes = md5hash(ByteBuffer.wrap(key)).toString().getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
            
            ByteBuffer hashBuf = ByteBuffer.allocate(hashBytes.length+delimeterBytes.length);
            hashBuf.put(hashBytes);
            hashBuf.put(delimeterBytes);
            hashBuf.flip();
            
            return hashBuf;
    }

    public static ByteBuffer hashKeyBytes(byte[]... keys)
    {
        byte hashedKey[] = null;

        if (keys.length <= 1 || !Arrays.equals(keys[keys.length - 2], delimeterBytes))
            throw new IllegalStateException("malformed key");

        byte[] indexName = keys[0];

        if (indexHashingEnabled)
        {
            int delimiterCount = 1;
            for (int i = 0; i < keys.length - 2; i++)
            {

                if (Arrays.equals(keys[i], delimeterBytes))
                {
                    delimiterCount++;
                }
            }

            if (delimiterCount > 2)
                throw new IllegalStateException("key contains too many delimiters");

            indexName = hashBytes(indexName).array(); // safe, uses .wrap
        }

        // no hashing, just combine the arrays together
        int totalBytes = indexName.length;
        for (int i = 1; i < keys.length; i++){
         
            //for index hashing we've already add the delimiter
            if(indexHashingEnabled && i == 1)
                continue;
            
            totalBytes += keys[i].length;
        }

        hashedKey = new byte[totalBytes];
        System.arraycopy(indexName, 0, hashedKey, 0, indexName.length);
        int currentLen = indexName.length;

        for (int i = 1; i < keys.length; i++)
        {
            
            //for index hashing we've already add the delimiter
            if(indexHashingEnabled && i == 1)
                continue;
            
            System.arraycopy(keys[i], 0, hashedKey, currentLen, keys[i].length);
            currentLen += keys[i].length;
        }

       
        return ByteBuffer.wrap(hashedKey);
    }

    public static int mreadVInt(ByteBuffer buf)
    {       
        int length = buf.remaining();
        
        if(length == 0)
            return 0;
        
        byte b = buf.get();
        int i = b & 0x7F;
        for (int pos = 1, shift = 7; (b & 0x80) != 0 && pos < length; shift += 7, pos++)
        {
            b = buf.get();
            i |= (b & 0x7F) << shift;
        }

        return i;
    }
    
    public static int readVInt(ByteBuffer buf)
    {       
        int length = buf.remaining();
        
        if(length == 0)
            return 0;
        
        byte b = buf.get(buf.position());
        int i = b & 0x7F;
        for (int pos = 1, shift = 7; (b & 0x80) != 0 && pos < length; shift += 7, pos++)
        {
            b = buf.get(buf.position() + pos);
            i |= (b & 0x7F) << shift;
        }

        return i;
    }

    public static byte[] writeVInt(int i)
    {
        int length = 0;
        int p = i;

        while ((p & ~0x7F) != 0)
        {
            p >>>= 7;
            length++;
        }
        length++;

        byte[] buf = new byte[length];
        int pos = 0;
        while ((i & ~0x7F) != 0)
        {
            buf[pos] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
            pos++;
        }
        buf[pos] = (byte) i;

        return buf;
    }
}
