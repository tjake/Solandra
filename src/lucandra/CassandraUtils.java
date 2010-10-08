/**
 * Copyright 2009 T Jake Luciani
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CassandraUtils {

    public static final String keySpace            = System.getProperty("lucandra.keyspace", "Lucandra");
    public static final String termVecColumnFamily = "TermInfo";
    public static final String docColumnFamily     = "Documents";
    
    public static final String positionVectorKey   = "Position";
    public static final String offsetVectorKey     = "Offsets";
    public static final String termFrequencyKey    = "Frequencies";
    public static final String normsKey            = "Norms";
    
    public static final byte[] emptyByteArray      = new byte[]{}; 
    public static final List<Number> emptyArray    = Arrays.asList( new Number[]{0} );
    public static final String delimeter           = new String("\uffff");
    public static final byte[] delimeterBytes;
    
    public static final String finalToken          = new String("\ufffe\ufffe");
    public static final byte[] finalTokenBytes; 

    public static final List<byte[]> allTermColumns = Arrays.asList(
            CassandraUtils.termFrequencyKey.getBytes(),
            CassandraUtils.positionVectorKey.getBytes(),
            CassandraUtils.normsKey.getBytes(),
            CassandraUtils.offsetVectorKey.getBytes());
    
    public static final String documentIdField     = System.getProperty("lucandra.id.field",delimeter+"KEY"+delimeter);
    public static final String documentMetaField   = delimeter+"META"+delimeter;
    
    public static final boolean indexHashingEnabled = Boolean.valueOf(System.getProperty("index.hashing","true"));
    
    public static final ColumnPath metaColumnPath;
    static{
        try {
            delimeterBytes = delimeter.getBytes("UTF-8");
            metaColumnPath = new ColumnPath(CassandraUtils.docColumnFamily).setColumn(documentMetaField.getBytes());
            finalTokenBytes = finalToken.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported by this JVM");
        }
    }

    public static final String hashChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    public static final BigInteger CHAR_MASK = new BigInteger("65535");

    
    private static final Logger logger = Logger.getLogger(CassandraUtils.class);

    /*This can be used to communicate without thrift transport, just POJOs
     * 
     *
    public static Cassandra.Iface createFatConnection() throws IOException {
             
        StorageService.instance.initClient();
        
        //Wait for gossip
        try
        {
            logger.info("Waiting 10s for gossip to complete...");
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
        }
        
        return new CassandraServer();      
    }*/
    
    
    public static Cassandra.Iface createConnection() throws TTransportException {


        if(System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null) {
           logger.warn("cassandra.host or cassandra.port is not defined, using default");
        }


        return createRobustConnection( System.getProperty("cassandra.host","localhost"),
                                 Integer.valueOf(System.getProperty("cassandra.port","9160")),
                                 Boolean.valueOf(System.getProperty("cassandra.framed", "true")),
                                 true);
    }
    
    
    public static Cassandra.Iface createRobustConnection(String host, Integer port, boolean framed, boolean randomizeConnections) {
        
        return CassandraProxyClient.newInstance(host, port, framed, randomizeConnections);
    }
    
    public static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws TTransportException {

        //connect to cassandra                                                                                                                              
        TSocket socket = new TSocket(host, port);
        TTransport trans;

        if(framed)
            trans = new TFramedTransport(socket);
        else
            trans = socket;

        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);     
        
        Cassandra.Client client =  new Cassandra.Client(protocol);
    
        try {
            client.set_keyspace(CassandraUtils.keySpace);
        } catch (InvalidRequestException e) {
            throw new TTransportException(e);
        } catch (TException e) {
            throw new TTransportException(e);
        }
        
        return client;
    }
   
    public static Term parseTerm(String termStr) {
             
        int index = termStr.indexOf(delimeter);
             
        if (index < 0){
            throw new RuntimeException("invalid term format: "+index+" " + termStr);
        }

        return new Term(termStr.substring(0,index), termStr.substring(index+delimeter.length()));
    }

    public static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public static final int byteArrayToInt(byte[] b) {
        return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF);
    }

    public static final byte[] intVectorToByteArray(List<Number> intVector) {
        
        if(intVector.size() == 0)
            return emptyByteArray;
        
        if(intVector.get(0) instanceof Byte)
            return new byte[]{intVector.get(0).byteValue()};
        
        ByteBuffer buffer = ByteBuffer.allocate(4 * intVector.size());

        for (Number i : intVector) {
            buffer.putInt(i.intValue());
        }

        return buffer.array();
    }
    
    public static final Comparator<byte[]> byteArrayComparator = new Comparator<byte[]>()
    {
        public int compare(byte[] o1, byte[] o2)
        {
            return compareByteArrays(o1, o2);
        }
    };

    public static int compareByteArrays(byte[] bytes1, byte[] bytes2) {

        if(null == bytes1){
            if(null == bytes2) return 0;
            else return -1;
        }
        if(null == bytes2) return 1;

        int minLength = Math.min(bytes1.length, bytes2.length);
        for(int i = 0; i < minLength; i++)
        {
            if(bytes1[i] == bytes2[i])
                continue;
            // compare non-equal bytes as unsigned
            return (bytes1[i] & 0xFF) < (bytes2[i] & 0xFF) ? -1 : 1;
        }
        if(bytes1.length == bytes2.length) return 0;
        else return (bytes1.length < bytes2.length)? -1 : 1;

    }

    public static final int[] byteArrayToIntArray(byte[] b) {

        if (b.length % 4 != 0)
            throw new RuntimeException("Not a valid int array:" + b.length);

        int[] intArray = new int[b.length / 4];
        int idx = 0;

        for (int i = 0; i < b.length; i += 4) {
            intArray[idx++] = (b[i] << 24) + ((b[i + 1] & 0xFF) << 16) + ((b[i + 2] & 0xFF) << 8) + (b[i + 3] & 0xFF);
        }

        return intArray;
    }

    public static final byte[] encodeLong(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(8);

        buffer.putLong(l);

        return buffer.array();
    }

    public static final long decodeLong(byte[] bytes) {

        if (bytes.length != 8)
            throw new RuntimeException("must be 8 bytes");

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        return buffer.getLong();
    }

    public static final byte[] encodeUUID(UUID docUUID) {

        ByteBuffer buffer = ByteBuffer.allocate(16);

        buffer.putLong(docUUID.getMostSignificantBits());
        buffer.putLong(docUUID.getLeastSignificantBits());
        return buffer.array();
    }

    public static final UUID readUUID(byte[] bytes) {

        if (bytes.length != 16)
            throw new RuntimeException("uuid must be exactly 16 bytes");

        return UUID.nameUUIDFromBytes(bytes);

    }

    public static void addToMutationMap(Map<byte[],Map<String,List<Mutation>>> mutationMap, String columnFamily, byte[] column, byte[] key, byte[] value, Map<String,List<Number>> superColumns){
        
        Long clock = System.currentTimeMillis();
        
        Map<String,List<Mutation>> cfMutation = mutationMap.get(key);
        
        if(cfMutation == null){
            cfMutation = new HashMap<String,List<Mutation>>();
            mutationMap.put(key, cfMutation);
        }
       
        Mutation mutation = new Mutation();
        
        List<Mutation> mutationList = cfMutation.get(columnFamily);
        if(mutationList == null) {
            mutationList = new ArrayList<Mutation>();
            cfMutation.put(columnFamily, mutationList);
        }
        
        
        if(value == null && superColumns == null){ //remove
            
            Deletion d = new Deletion();
            
            d.setTimestamp(clock);
            
            if(column != null){
                try {
                    String skey = new String(key,"UTF-8");
                    logger.debug("deleting "+skey+"("+new String(column,"UTF-8")+")");
                } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                d.setSuper_column(column);

                //FIXME: Somthing ain't right, we shouldn't need to specify these to work
                d.setPredicate(new SlicePredicate().setColumn_names(allTermColumns));
            }else{
                d.setPredicate(new SlicePredicate().setSlice_range(new SliceRange(new byte[]{}, new byte[]{},false,Integer.MAX_VALUE)));
            }
            
            mutation.setDeletion(d);
       
        }else{ //insert
                
            ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
            
            if(superColumns == null){
            
                //check for multi valued fields
                for(Mutation m : mutationList){
                    if(Arrays.equals(m.getColumn_or_supercolumn().getColumn().getName(),column)){
                        byte[] currentValue = m.getColumn_or_supercolumn().getColumn().getValue();
                        
                        byte[] newValue = new byte[currentValue.length + delimeterBytes.length + value.length - 1 ];
                        System.arraycopy(currentValue, 0, newValue, 0, currentValue.length-1);
                        System.arraycopy(delimeterBytes, 0, newValue, currentValue.length-1, delimeterBytes.length);
                        System.arraycopy(value, 0, newValue, currentValue.length+delimeterBytes.length-1, value.length);
                        
                        m.getColumn_or_supercolumn().getColumn().setValue(newValue);
                    
                        //done
                        return;
                    }
                }
                
                
                cc.setColumn(new Column(column, value, clock));                    
            
            } else {
                
                SuperColumn sc = new SuperColumn();
                List<Column> columns = new ArrayList<Column>();
                
                sc.setName(column);
                sc.setColumns(columns);
                
                for(Map.Entry<String, List<Number>> e : superColumns.entrySet()){        
                    
                    try {
                        columns.add(new Column(e.getKey().getBytes("UTF-8"), intVectorToByteArray(e.getValue()), clock));
                    } catch (UnsupportedEncodingException e1) {
                        throw new RuntimeException("UTF-8 not supported by this JVM");
                    }
                }
                                        
                cc.setSuper_column(sc);      
            }
            
            mutation.setColumn_or_supercolumn(cc);
        }
        
        
        mutationList.add(mutation);       
    }
    
    public static void robustBatchInsert(Cassandra.Iface client, Map<byte[],Map<String,List<Mutation>>> mutationMap) {

        // Should use a circut breaker here
        boolean try_again = false;
        int attempts = 0;
        long startTime = System.currentTimeMillis();
        do {
            try {
                attempts++;
                try_again = false;
                client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
                
                mutationMap.clear();
                //if(logger.isDebugEnabled())
                //    logger.debug("Inserted in " + (startTime - System.currentTimeMillis()) / 1000 + "ms");
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (UnavailableException e) {
                try_again = true;
            } catch (TimedOutException e) {
                try_again = true;
            }
        } while (try_again && attempts < 10);
        
        //fail
        if(try_again){
            throw new RuntimeException("Insert still failed after 10 attempts");
        }
    }
    
    /** Read the object from bytes string. */
    public static Object fromBytes(byte[] data) throws IOException,
            ClassNotFoundException {
 
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();
        return o;
    }
 
    /** Write the object to bytes. */
    public static byte[] toBytes(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return baos.toByteArray();
    }
    
    public static byte[] hashKeyBytes(byte[]... keys){
        byte hashedKey[] = null;
        
        if(keys.length <= 1 || !Arrays.equals(keys[keys.length-2], delimeterBytes))
            throw new IllegalStateException("malformed key");

        if(indexHashingEnabled){
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("SHA");
            } catch (NoSuchAlgorithmException e) {
               throw new RuntimeException(e);
            }
            
            int saltSize = 0;
            int delimiterCount = 1;
            for(int i=0; i<keys.length-2; i++){
                
                if(Arrays.equals(keys[i], delimeterBytes)){
                    delimiterCount++;
                }
                
                saltSize += keys[i].length;                
            }
            
            if(delimiterCount > 2)
                throw new IllegalStateException("key contains too many delimiters");

            byte[] salt = new byte[saltSize];
            int currentLen = 0;
            for(int i=0; i<keys.length-2;  i++){
                    System.arraycopy(keys[i], 0, salt, currentLen, keys[i].length);
                    currentLen += keys[i].length;            
            }
                
            md.update(salt);
           
            byte[] hashPart  = bytesForBig(new BigInteger(1, md.digest()), 8);            
            byte[] otherPart = null;
            
            if(delimiterCount == 2){
                otherPart = new byte[ keys[keys.length-3].length + delimeterBytes.length + keys[keys.length-1].length];

                int len = 0;
  
                System.arraycopy(keys[keys.length-3], 0, otherPart, len, keys[keys.length-3].length);
                
                len += keys[keys.length-3].length;
                
                System.arraycopy(delimeterBytes, 0, otherPart, len, delimeterBytes.length);
                
                len += delimeterBytes.length;
                
                System.arraycopy(keys[keys.length-1], 0, otherPart, len, keys[keys.length-1].length);
            }else{
                otherPart = keys[keys.length-1];
            }
            
            hashedKey = new byte[hashPart.length + delimeterBytes.length + otherPart.length];
            
            
            //combine the 3 parts
            System.arraycopy(hashPart, 0, hashedKey, 0, hashPart.length);            
            System.arraycopy(delimeterBytes, 0, hashedKey, hashPart.length, delimeterBytes.length);
            System.arraycopy(otherPart, 0, hashedKey, hashPart.length+delimeterBytes.length, otherPart.length);
                 
        } else {
            
            //no hashing, just combine the arrays together
            
            int totalBytes = 0;
            for(int i=0; i<keys.length; i++)
                totalBytes += keys[i].length;
            
            hashedKey = new byte[totalBytes];
            int currentLen = 0;
            for(int i=0; i<keys.length; i++){
                System.arraycopy(keys[i], 0, hashedKey, currentLen, keys[i].length);
                currentLen += keys[i].length;
            }
        }
        
        return hashedKey;
    }
    
    
    /*public static String hashKey(String key) {
        
        if(!indexHashingEnabled)
            return key;
        
        try {
            MessageDigest md = MessageDigest.getInstance("SHA");
            
            //Please fixme: this is so hard to read 
            //should have separte hashing functions for terms and docs
            //terms look like: indexname/field/term
            //docs look like: indexname/docid
            int indexPoint = key.indexOf(delimeter);
            int breakPoint = key.lastIndexOf(delimeter);
                       
            if(breakPoint == -1)
                throw new IllegalStateException("key does not contain delimiter");
            
                      
            String salt = key.substring(0,breakPoint);
            
            try {
                md.update(salt.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 not supported by this JVM");
            }
            
            return stringForBig( new BigInteger(1,md.digest()),8)+key.substring(indexPoint);
            
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        
    }*/
    
    //based on cassandra source
    private static byte[] bytesForBig(BigInteger big, int sigchars)
    {
        byte[] bytes = new byte[sigchars];
             
        for (int i = 0; i < sigchars; i++)
        {
            int maskpos = 16 * (sigchars - (i + 1));
            // apply bitmask and get char value
            bytes[i] = (byte)hashChars.charAt(big.and(CHAR_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() % hashChars.length());
        }
        return bytes;
    }
    
}
