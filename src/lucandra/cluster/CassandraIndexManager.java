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
package lucandra.cluster;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lucandra.CassandraUtils;

import com.google.common.collect.MapMaker;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;


//Instead of grabbing all of them just grab a contiguous slab via offset
public class CassandraIndexManager
{

    // To increase throughput we distribute docs across a number of shards at
    // once
    // The idea being different shards live on different boxes
    protected final int                            shardsAtOnce;

    private int[]                                  randomSeq;

    public static final int                        maxDocsPerShard = Integer.valueOf(CassandraUtils.properties
                                                                           .getProperty(
                                                                                   "solandra.maximum.docs.per.shard",
                                                                                   "131072"));
    public static final int                        reserveSlabSize = Integer.valueOf(CassandraUtils.properties
                                                                           .getProperty(
                                                                                   "solandra.index.id.reserve.size",
                                                                                   "16384"));

    private final int                              offsetSlots     = maxDocsPerShard / reserveSlabSize;
    public final int                              expirationTime  = 120;                                          // seconds

    private final ConcurrentMap<String, AllNodeRsvps>  indexReserves = new MapMaker().makeMap();
    
    private final ConcurrentMap<String, ShardInfo> indexShards     = new MapMaker().makeMap();

    private static final Logger                    logger          = Logger.getLogger(CassandraIndexManager.class);

    private class ShardInfo
    {
        public final String                                   indexName;
        public final long                                     ttl    = System.currentTimeMillis()
                                                                             + (expirationTime * 1000) - 1000;
        public final ConcurrentSkipListMap<Integer, NodeInfo> shards = new ConcurrentSkipListMap<Integer, NodeInfo>();

        public ShardInfo(String indexName)
        {
            this.indexName = indexName;
        }
    }

    private class NodeInfo
    {
        public Integer                    shard;
        public Map<String, AtomicInteger> nodes = new HashMap<String, AtomicInteger>();

        public NodeInfo(Integer shard)
        {
            this.shard = shard;
        }
    }

    private class AllNodeRsvps
    {
        public final AtomicLong incrementor  = new AtomicLong(0);
        public final List<RsvpInfo> rsvpList = new ArrayList<RsvpInfo>();  
        
        public Long getNextId()
        {            
            if(rsvpList.isEmpty())
                return null;
            
            long start = incrementor.incrementAndGet();
            long len   = rsvpList.size();
            long end   = start + len;
            
            for(long i=start; i<end; i++)
            {
                int pos = (int)(i % len);
                
                RsvpInfo info = rsvpList.get(pos);
            
                if(info == null)
                    continue;
                
                //clear expired ids
                if(info.ttl < System.currentTimeMillis())
                {
                    rsvpList.set(pos, null);
                }
                
                int nextId = info.currentId.incrementAndGet();
                
                if(nextId <= info.endId)
                {
                    return (long)(maxDocsPerShard * info.node.shard) + nextId;
                }
                else
                {
                    rsvpList.set(pos, null);
                }              
            }
                   
            return null;
        }
    }
    
    private class RsvpInfo
    {
        public NodeInfo      node;
        public AtomicInteger currentId;
        public final int     endId;
        public final long    ttl = System.currentTimeMillis() + (expirationTime * 1000);

        public RsvpInfo(int startId, int endId, NodeInfo node)
        {
            currentId = new AtomicInteger(startId);
            this.endId = endId;
            this.node  = node;
        }
    }

    public CassandraIndexManager(int shardsAtOnce)
    {
        this.shardsAtOnce = shardsAtOnce;

        logger.info("Shards at once: " + shardsAtOnce);

        // get our unique sequence
        Random r = null;
        try
        {
            r = new Random(getNodeSeed(getToken()));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        randomSeq = new int[offsetSlots];

        for (int i = 0, offset = 0; i < CassandraIndexManager.maxDocsPerShard; i++)
        {
            if (i % reserveSlabSize == 0)
            {
                randomSeq[offset] = i;
                offset++;
            }
        }

        randomSeq = shuffle(randomSeq, r);
    }

    private ShardInfo getShardInfo(String indexName, boolean force) throws IOException
    {

        ShardInfo shards = indexShards.get(indexName);
        ShardInfo currentShards = shards;

        if (shards != null && !force)
        {
            if (shards.ttl > System.currentTimeMillis())
            {
                return shards;
            }
            else
            {
                logger.info("ShardInfo for " + indexName + " has expired");
            }
        }

        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes,
                "shards".getBytes("UTF-8"));

        ReadCommand cmd = new SliceFromReadCommand(CassandraUtils.keySpace, key, new ColumnParent(
                CassandraUtils.schemaInfoColumnFamily), ByteBufferUtil.EMPTY_BYTE_BUFFER,
                ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);

        List<Row> rows = CassandraUtils.robustRead(ConsistencyLevel.QUORUM, cmd);

        shards = new ShardInfo(indexName);
        if (rows != null || !rows.isEmpty())
        {
            assert rows.size() == 1;

            Row row = rows.get(0);

            if (row.cf != null && !row.cf.isMarkedForDelete())
            {

                assert row.cf.getSortedColumns() != null;

                // Each column represents each shard and latest id for each
                // node
                // {"shard1" : {"node1" : 1234}}
                for (IColumn c : row.cf.getSortedColumns())
                {
                    String shardStr = ByteBufferUtil.string(c.name());
                    Integer shardNum = Integer.valueOf(shardStr);
                    
                    assert c instanceof SuperColumn;
                    
                    NodeInfo nodes = new NodeInfo(shardNum);
                    
                    for(IColumn subCol : c.getSubColumns())
                    {
                        
                        String token = ByteBufferUtil.string(subCol.name());
                        AtomicInteger offset = new AtomicInteger(Integer.valueOf(ByteBufferUtil.string(subCol.value())));
                    
                       
                        nodes.nodes.put(token, offset);
                      
                        shards.shards.put(shardNum, nodes);               
                    }
                }
            }
        }

        if (currentShards == null)
        {
            if (indexShards.putIfAbsent(indexName, shards) == null)
                return shards;
        }
        else if (indexShards.replace(indexName, currentShards, shards))
        {

            logger.info(indexName + " has " + shards.shards.size() + " shards");

            return shards;
        }

        return indexShards.get(indexName);

    }

    public void deleteId(String indexName, long id)
    {

    }

    public long getMaxId(String indexName) throws IOException
    {
        // find the max shard
        ShardInfo shards = getShardInfo(indexName, false);

        if (shards.shards.isEmpty())
            return 0;

        int highest = 0;

        // Find the highest *used* slab
        // loop is ordered by slab number
        for (Map.Entry<Integer, NodeInfo> e : shards.shards.entrySet())
        {
            Integer currentOffset = null;
            for (Map.Entry<String, AtomicInteger> e1 : e.getValue().nodes.entrySet())
            {
                if (e1.getValue().get() > 0)
                {
                    currentOffset = e1.getValue().get();
                    break;
                }
            }

            if (currentOffset != null)
                highest = e.getKey();
        }

        return (CassandraIndexManager.maxDocsPerShard * highest);
    }

    public Long getId(String indexName, String key) throws IOException
    {
        return checkForUpdate(indexName, key);
    }

    public Long checkForUpdate(String indexName, String key) throws IOException
    {
        ByteBuffer keyCol = ByteBuffer.wrap(key.getBytes("UTF-8"));
        ByteBuffer keyKey = CassandraUtils.hashKeyBytes((indexName + "~" + key).getBytes("UTF-8"),
                CassandraUtils.delimeterBytes, "keys".getBytes("UTF-8"));

        List<Row> rows = CassandraUtils.robustRead(keyKey, new QueryPath(CassandraUtils.schemaInfoColumnFamily), Arrays
                .asList(keyCol), ConsistencyLevel.QUORUM);

        if (rows.size() == 1)
        {
            Row row = rows.get(0);

            if (row.cf != null)
            {
                IColumn col = row.cf.getColumn(keyCol);

                if (col != null)
                {
                    Collection<IColumn> subCols = col.getSubColumns();

                    if (subCols != null && !subCols.isEmpty())
                    {
                        ByteBuffer idVal = col.getSubColumns().iterator().next().name();
                        Long id = Long.valueOf(ByteBufferUtil.string(idVal));

                        return id;
                    }
                }
            }
        }

        return null;
    }

    public String getToken()
    {
        if (StorageService.instance.isClientMode())
            return CassandraUtils.fakeToken;

        return StorageService.instance.getTokenMetadata().getToken(FBUtilities.getLocalAddress()).toString();
    }

    public long getNextId(String indexName, String key, RowMutation[] rowMutations) throws IOException
    {
        if (rowMutations.length != 3)
            throw new IllegalArgumentException("rowMutations must be length 3");

        String myToken = getToken();
        ShardInfo shards = null;
        NodeInfo nodes[] = null;
        Long id = null;

        int attempts = 0;
        while (attempts < CassandraUtils.retryAttempts)
        {
            shards = getShardInfo(indexName, false);
            nodes = pickAShard(shards);

            id = nextReservedId(indexName, nodes, myToken);

            if (id == null)
            {
                attempts++;
                // logger.info("Failed to get an ID, trying again");
                continue;
            }
            break;
        }

        if (id == null)
            throw new IllegalStateException(myToken + ": Unable to reserve an id");

        int shard     = getShardFromDocId(id);
        int shardedId = getShardedDocId(id);
        
        ByteBuffer idCol = ByteBufferUtil.bytes(String.valueOf(shardedId));
        ByteBuffer keyCol = ByteBuffer.wrap(key.getBytes("UTF-8"));

        // Permanently mark the id as taken
        ByteBuffer idKey = CassandraUtils.hashKeyBytes((indexName + "~" + shard).getBytes("UTF-8"),
                CassandraUtils.delimeterBytes, "ids".getBytes("UTF-8"));

        RowMutation rm = new RowMutation(CassandraUtils.keySpace, idKey);
        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, idCol, ByteBuffer.wrap(myToken.getBytes("UTF-8"))),
                keyCol, System.currentTimeMillis());

        // Permanently link the key to the id
        ByteBuffer keyKey = CassandraUtils.hashKeyBytes((indexName + "~" + key).getBytes("UTF-8"),
                CassandraUtils.delimeterBytes, "keys".getBytes("UTF-8"));


        ByteBuffer idVal = ByteBuffer.wrap(id.toString().getBytes("UTF-8"));

        RowMutation rm2 = new RowMutation(CassandraUtils.keySpace, keyKey);
        rm2.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, keyCol, idVal), CassandraUtils.finalTokenBytes,
                System.currentTimeMillis());

        // Update last offset info for this shard
        RowMutation rm3 = updateNodeOffset(indexName, myToken, shard, shardedId);

        rowMutations[0] = rm;
        rowMutations[1] = rm2;
        rowMutations[2] = rm3;

        return id;
    }

    public long getNextId(String indexName, String key) throws IOException
    {

        RowMutation[] rms = new RowMutation[3];

        Long val = getNextId(indexName, key, rms);

        // TODO: Delayed Insert!
        // Checks for more recent updates and disregards the older ones

        CassandraUtils.robustInsert(CassandraUtils.consistency, rms);
        return val;
    }

    public void resetCounter(String indexName) throws IOException
    {
        // update all shards to 0 for all tokens
        ShardInfo shards = getShardInfo(indexName, true);

        List<RowMutation> rms = new ArrayList<RowMutation>();

        for (NodeInfo nodes : shards.shards.values())
        {
            for (String token : nodes.nodes.keySet())
                rms.add(updateNodeOffset(indexName, token, nodes.shard, randomSeq[0]));
        }

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rms.toArray(new RowMutation[] {}));
    }

    private Long nextReservedId(String indexName, NodeInfo[] shards, String myToken) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("in reserveIds for index " + indexName);

        AllNodeRsvps currentRsvpd = indexReserves.get(indexName);

        if (currentRsvpd != null)
        {                      
            Long nextId = currentRsvpd.getNextId();
            
            if(nextId != null)
                return nextId;
            
            if (logger.isDebugEnabled())
                logger.debug("need more ids for " + myToken);           
        }

        AllNodeRsvps allNewRsvps = new AllNodeRsvps();
        
        // Pick a new shard
        for (NodeInfo node : shards)
        {
            AtomicInteger offset = node.nodes.get(myToken);

            assert offset != null;

            int startingOffset = offset.get();
            int nextOffset = startingOffset;

            // goto next offset marker (unless its the first or last)
            int randomSequenceOffset = getRandomSequenceOffset(startingOffset);

            if (startingOffset != randomSeq[0])
            {
                if (randomSequenceOffset != (offsetSlots - 1))
                {
                    nextOffset = randomSeq[randomSequenceOffset + 1];
                }
                else
                {
                    continue;
                }
            }

            // logger.info(myToken+ "  startingOffset = "+startingOffset+
            // ", nextOffset = "+nextOffset);

           
            
            synchronized (node)
            {
                // First, make sure another thread didn't already do this work
                AllNodeRsvps possiblyNewRsvpd = indexReserves.get(indexName);
                if (possiblyNewRsvpd != currentRsvpd || startingOffset != offset.get())
                {
                    return possiblyNewRsvpd == null ? null : possiblyNewRsvpd.getNextId();
                }

                
                ByteBuffer key = CassandraUtils.hashKeyBytes((indexName + "~" + node.shard).getBytes("UTF-8"),
                        CassandraUtils.delimeterBytes, "ids".getBytes("UTF-8"));

                // Write the reserves
                RowMutation rm = new RowMutation(CassandraUtils.keySpace, key);

                ByteBuffer id = ByteBufferUtil.bytes(String.valueOf(nextOffset));
                ByteBuffer off = id;

                rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, id, ByteBuffer.wrap(myToken
                        .getBytes("UTF-8"))), off, System.currentTimeMillis(), expirationTime);

                CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);

                // Read the columns back
                IColumn supercol = null;
                int attempts = 0;
                while (supercol == null && attempts < CassandraUtils.retryAttempts)
                {
                    try
                    {
                        List<Row> rows = CassandraUtils.robustRead(key, new QueryPath(
                                CassandraUtils.schemaInfoColumnFamily), Arrays.asList(id), ConsistencyLevel.QUORUM);

                        if (rows == null || rows.size() == 0)
                        {
                            continue;
                        }

                        if (rows.size() == 1)
                        {
                            Row row = rows.get(0);

                            if (row.cf == null || row.cf.isMarkedForDelete())
                            {
                                continue;
                            }

                            supercol = rows.get(0).cf.getColumn(id);
                        }
                    }
                    catch (IOException e)
                    {
                        // let's try again...
                    }

                    attempts++;
                }

                if (supercol == null)
                    throw new IllegalStateException("just wrote " + offset + ", but didn't read it");

                long minTtl = Long.MAX_VALUE;
                ByteBuffer winningToken = null;

                // See which ones we successfully reserved
                for (IColumn c : supercol.getSubColumns())
                {

                    // someone already took this id
                    if (!(c instanceof ExpiringColumn) && !(c instanceof DeletedColumn))
                    {
                        if (logger.isDebugEnabled())
                            try
                            {
                                logger.debug(offset + " was taken by " + ByteBufferUtil.string(c.name()));
                            }
                            catch (CharacterCodingException e)
                            {

                            }

                        winningToken = null;
                        break;
                    }

                    // expired reservation
                    if (c.isMarkedForDelete())
                        continue;

                    if (c.timestamp() == minTtl && winningToken.compareTo(c.name()) <= 0)
                    {
                        winningToken = c.name();
                    }

                    if (c.timestamp() < minTtl)
                    {
                        minTtl = c.timestamp();
                        winningToken = c.name();
                    }
                }

                String winningTokenStr;
                try
                {
                    winningTokenStr = winningToken == null ? "" : ByteBufferUtil.string(winningToken);
                }
                catch (CharacterCodingException e)
                {
                    throw new RuntimeException(e);
                }

                // we won!
                if (winningToken != null && winningTokenStr.equals(myToken))
                {
                    int numReserved = nextOffset;
                    for (int i = nextOffset; i == nextOffset || i % reserveSlabSize != 0; i++)
                    {                        
                        numReserved++;
                    }
                    
                    allNewRsvps.rsvpList.add(new RsvpInfo(nextOffset, numReserved, node));

                    logger.info("Reserved " + numReserved + " ids for " + myToken + " shard " + node.shard
                            + " from slot " + getRandomSequenceOffset(nextOffset));
                }
                else
                {
                    // we lost, try try again...

                    // secial case, otherwise we never move on
                    if (nextOffset == randomSeq[0])
                        nextOffset += 1;

                    // mark this offset as taken and move on
                    updateNodeOffset(indexName, myToken, node.shard, nextOffset);
                    continue;
                }


                if (logger.isDebugEnabled())
                    logger.debug("offset for shard " + node.shard + " " + nextOffset);
            }
        }


        // check that offset is the same as when we started
        if (currentRsvpd == null)
        {
            if (indexReserves.putIfAbsent(indexName, allNewRsvps) != null)
            {
                logger.info("reserves changed, using those instead");
                allNewRsvps = indexReserves.get(indexName);
            }
        }
        else
        {
            if (!indexReserves.replace(indexName, currentRsvpd, allNewRsvps))
            {
                logger.info("already reserved by someone else, using those");
                return indexReserves.get(indexName).getNextId();
            }
        }

        if (logger.isDebugEnabled())
            logger.debug("Reserved " + allNewRsvps.rsvpList.size() + " ids for " + myToken);

        return allNewRsvps.getNextId();
    }

    private int getRandomSequenceOffset(int offset)
    {
        if (offset >= CassandraIndexManager.maxDocsPerShard)
            throw new IllegalArgumentException("offset can not be > " + CassandraIndexManager.maxDocsPerShard);

        for (int randomSeqOffset = 0; randomSeqOffset < randomSeq.length; randomSeqOffset++)
        {
            int randomSequenceStart = randomSeq[randomSeqOffset];

            if (randomSequenceStart <= offset && offset < randomSequenceStart + reserveSlabSize)
                return randomSeqOffset;
        }

        throw new IllegalStateException("Unable to locate random sequence position for offset " + offset);
    }

    /**
     * Looks for appropriate shard to reserve ids from
     * 
     * TODO: calculate where other tokens are in their sequence
     * 
     * @param shards
     * @return
     */
    private NodeInfo[] pickAShard(ShardInfo shards) throws IOException
    {

        assert shards != null;

        synchronized (shards)
        {
            String myToken = getToken();

            NodeInfo[] picked = new NodeInfo[shardsAtOnce];

            int maxShard = -1;
            int pickedShard = 0;

            for (Map.Entry<Integer, NodeInfo> shard : shards.shards.entrySet())
            {
                NodeInfo nodes = shard.getValue();

                AtomicInteger offset = nodes.nodes.get(myToken);

                // new shard for this node
                if (offset == null)
                {
                    // this means shard was started by another node
                    offset = new AtomicInteger(randomSeq[0]);

                    logger.info("shard started by another node initializing with " + randomSeq[0]);

                    RowMutation rm = updateNodeOffset(shards.indexName, myToken, nodes.shard, offset.get());
                    CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);
                }

                int randomSeqOffset = getRandomSequenceOffset(offset.get());

                if (logger.isDebugEnabled())
                    logger.info(myToken + ": shard = " + shard.getKey() + ", offset = " + offset.get()
                            + ", offsetLookup = " + randomSeqOffset + ", offsetSlots =  " + offsetSlots);

                // can we still use this shard (other nodes havent gobbled up
                // the ids)?
                // if(randomSeqOffset+1 == offsetSlots)
                // logger.info(myToken+": shard = "+shard.getKey()+", offset = "+offset.get()+", offsetLookup = "+randomSeqOffset+", offsetSlots =  "+offsetSlots);

                if (randomSeqOffset + 1 < offsetSlots)
                {
                    picked[pickedShard] = nodes;
                    pickedShard++;
                    if (pickedShard >= shardsAtOnce)
                        return picked;
                }

                if (shard.getKey() > maxShard)
                    maxShard = shard.getKey();

            }

            // new shards
            for (int i = pickedShard; i < shardsAtOnce; i++)
            {
                picked[i] = addNewShard(shards.indexName);
            }

            return picked;
        }
    }

    private NodeInfo addNewShard(String indexName) throws IOException
    {
        ShardInfo shards = getShardInfo(indexName, false);

        // get max shard
        Integer maxShard = -1;

        if (!shards.shards.isEmpty())
        {
            Map.Entry<Integer, NodeInfo> max = shards.shards.lastEntry();

            Integer currentOffset = null;
            for (Map.Entry<String, AtomicInteger> e1 : max.getValue().nodes.entrySet())
            {
                if (e1.getValue().get() > 0)
                {
                    currentOffset = e1.getValue().get();
                    break;
                }
            }

            if (currentOffset != null && currentOffset > 0)
            {
                maxShard = max.getKey();
            }
        }

        NodeInfo nodes = new NodeInfo(maxShard + 1);

        NodeInfo dupNodes = null;
        if ((dupNodes = shards.shards.putIfAbsent(nodes.shard, nodes)) == null)
        {
            logger.info("added new shard for " + indexName + " " + nodes.shard + " with offset " + randomSeq[0]);

            RowMutation rm = updateNodeOffset(indexName, getToken(), nodes.shard, randomSeq[0]); // offset 0
       
            CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);          
        }

        return dupNodes == null ? nodes : dupNodes;
    }

    private RowMutation updateNodeOffset(String indexName, String myToken, Integer shard, Integer offset)
            throws IOException
    {
        // Update last offset info for this shard
        ByteBuffer shardKey = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes,
                "shards".getBytes("UTF-8"));
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, shardKey);

        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, ByteBuffer.wrap(String.valueOf(shard)
                .getBytes("UTF-8")), ByteBuffer.wrap(myToken.getBytes("UTF-8"))), ByteBuffer.wrap(String
                .valueOf(offset).getBytes("UTF-8")), System.currentTimeMillis());

        // update locally
        ShardInfo     si = getShardInfo(indexName, false);
        AtomicInteger o = null;
        NodeInfo      n = si.shards.get(shard);
        
        if(!indexName.contains("~")) 
        {    
            if (n != null)
                o = n.nodes.get(myToken);
            else
                throw new RuntimeException("missing node info");

            if (o == null)
                n.nodes.put(myToken, new AtomicInteger(offset));
            else
                o.set(offset);

            if (logger.isDebugEnabled())
                logger.debug("updated node offset for " + indexName + "(" + shard + ")(" + myToken + ") to " + offset);
        }
        else
        {
            throw new RuntimeException("inner shard offset update attempt: "+indexName);
        }
        
        
        return rm;
    }

    private int getNodeSeed(String token) throws IOException
    {
        // Calculate the seed from the token
        MessageDigest md;
        try
        {
            md = MessageDigest.getInstance("SHA");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

        md.update(token.getBytes("UTF-8"));

        return new BigInteger(1, md.digest()).intValue();
    }

    /**
     * When this node starts up, create a unique, but reproducible list of doc
     * ids.
     * 
     * from: http://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
     * 
     * This list is used to find new ids to reserve while minimizing collisions
     * across nodes
     * 
     * 
     * @param array
     * @param rng
     */
    private int[] shuffle(int array[], Random rng)
    {
        // Always place maxVal at end (to avoid NPE)

        // i is the number of items remaining to be shuffled.
        for (int i = array.length - 1; i > 1; i--)
        {
            // Pick a random element to swap with the i-th element.
            int j = rng.nextInt(i); // 0 <= j <= i-1 (0-based array)
            // Swap array elements.
            int tmp = array[j];
            array[j] = array[i - 1];
            array[i - 1] = tmp;
        }

        return array;
    }

    public static int getShardFromDocId(long docId)
    {
        return (int) Math.floor(docId / CassandraIndexManager.maxDocsPerShard);
    }

    public static int getShardedDocId(long docId)
    {
        return (int) docId % CassandraIndexManager.maxDocsPerShard;
    }
}
