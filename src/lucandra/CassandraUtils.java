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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;

public class CassandraUtils {

    public static final String keySpace = System.getProperty("lucandra.keyspace", "Lucandra");
    public static final String termVecColumnFamily = "TermInfo";
    public static final String docColumnFamily = "Documents";

    public static final String positionVectorKey = "Position";
    public static final String offsetVectorKey = "Offsets";
    public static final String termFrequencyKey = "Frequencies";
    public static final String normsKey = "Norms";

    public static final byte[] emptyByteArray = new byte[] {};
    public static final List<Number> emptyArray = Arrays.asList(new Number[] { 0 });
    public static final String delimeter = new String("\uffff");
    public static final byte[] delimeterBytes;

    public static final String finalToken = new String("\ufffe\ufffe");

    public static final String documentIdField = System.getProperty("lucandra.id.field", delimeter + "KEY" + delimeter);
    public static final String documentMetaField = delimeter + "META" + delimeter;
    public static final byte[] documentMetaFieldBytes;
    
    public static final boolean indexHashingEnabled = Boolean.valueOf(System.getProperty("index.hashing", "true"));

    public static final QueryPath metaColumnPath;
    static {
        try {
            delimeterBytes = delimeter.getBytes("UTF-8");
            documentMetaFieldBytes = documentMetaField.getBytes("UTF-8");
            metaColumnPath = new QueryPath(CassandraUtils.docColumnFamily);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported by this JVM");
        }
    }

    public static final String hashChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    public static final BigInteger CHAR_MASK = new BigInteger("65535");

    private static final Logger logger = Logger.getLogger(CassandraUtils.class);

    //Start Cassandra up!!!
    public static void startup(){
        try {
         // initialize keyspaces
            for (String table : DatabaseDescriptor.getTables())
            {
                Table.open(table);
            }

            // replay the log if necessary and check for compaction candidates
            CommitLog.recover();
            CompactionManager.instance.checkAllColumnFamilies();

            // start server internals
            StorageService.instance.initServer();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    
        
    }
    
    
    public static String createColumnName(Term term) {

        return createColumnName(term.field(), term.text());
    }

    public static String createColumnName(String field, String text) {

        // case of all terms
        if (field.equals("") || text == null)
            return delimeter;

        return field + delimeter + text;
    }

    public static Term parseTerm(String termStr) {

        int index = termStr.indexOf(delimeter);

        if (index < 0) {
            throw new RuntimeException("invalid term format: " + index + " " + termStr);
        }

        return new Term(termStr.substring(0, index), termStr.substring(index + delimeter.length()));
    }

    public static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public static final int byteArrayToInt(byte[] b) {
        return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF);
    }

    public static final byte[] intVectorToByteArray(List<Number> intVector) {

        if (intVector.size() == 0)
            return emptyByteArray;

        if (intVector.get(0) instanceof Byte)
            return new byte[] { intVector.get(0).byteValue() };

        ByteBuffer buffer = ByteBuffer.allocate(4 * intVector.size());

        for (Number i : intVector) {
            buffer.putInt(i.intValue());
        }

        return buffer.array();
    }

    public static boolean compareByteArrays(byte[] a, byte[] b) {

        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }

        return true;

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

   
    
    public static void addMutations(Map<String,RowMutation> mutationList, String columnFamily, byte[] column, String key, byte[] value,
            Map<String, List<Number>> superColumns) {

        //Find or create row mutation
        RowMutation rm = mutationList.get(key); 
        if(rm == null){
            rm = new RowMutation(CassandraUtils.keySpace, key);
            mutationList.put(key, rm);
        }
        
        if (value == null && superColumns == null) { // remove

            if (column != null) {
                rm.delete(new QueryPath(columnFamily, column), System.currentTimeMillis());
            } else {
                rm.delete(new QueryPath(columnFamily), System.currentTimeMillis());
            }

        } else { // insert

            if (superColumns == null) {

                rm.add(new QueryPath(columnFamily,null, column), value, System.currentTimeMillis());

            } else {

                for (Map.Entry<String, List<Number>> e : superColumns.entrySet()) {

                    try {
                        rm.add(new QueryPath(columnFamily,column,e.getKey().getBytes("UTF-8")), intVectorToByteArray(e.getValue()), System.currentTimeMillis());
                    } catch (UnsupportedEncodingException ex) {
                        throw new RuntimeException("This JVM doesn't support UTF-8 Encoding");
                    }
                }
            }
        }
    }

    public static void robustInsert(Map<String,RowMutation> mutations) {

        int attempts = 0;
        while (attempts++ < 10) {

            try {
                StorageProxy.mutateBlocking(Arrays.asList(mutations.values().toArray(new RowMutation[]{})), ConsistencyLevel.ONE);
                mutations.clear();
                return;
            } catch (UnavailableException e) {
                
            } catch (TimeoutException e) {
               
            }
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
               
            }           
        }      
        
        throw new RuntimeException("insert failed after 10 attempts");
    }

    /** Read the object from bytes string. */
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {

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

    public static String hashKey(String key) {

        if (!indexHashingEnabled)
            return key;

        try {
            MessageDigest md = MessageDigest.getInstance("SHA");

            // Please fixme: this is so hard to read
            // should have separte hashing functions for terms and docs
            // terms look like: indexname/field/term
            // docs look like: indexname/docid
            int indexPoint = key.indexOf(delimeter);
            int breakPoint = key.lastIndexOf(delimeter);

            if (breakPoint == -1)
                throw new IllegalStateException("key does not contain delimiter");

            String salt = key.substring(0, breakPoint);

            try {
                md.update(salt.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 not supported by this JVM");
            }

            return stringForBig(new BigInteger(1, md.digest()), 8) + key.substring(indexPoint);

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }

    }

    // based on cassandra source
    private static String stringForBig(BigInteger big, int sigchars) {
        char[] chars = new char[sigchars];

        for (int i = 0; i < sigchars; i++) {
            int maskpos = 16 * (sigchars - (i + 1));
            // apply bitmask and get char value
            chars[i] = hashChars.charAt(big.and(CHAR_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() % hashChars.length());
        }
        return new String(chars);
    }

}
