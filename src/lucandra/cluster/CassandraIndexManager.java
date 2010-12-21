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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import lucandra.CassandraUtils;

import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceFromReadCommand;
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
  
    //To increase throughput we distribute docs across a number of shards at once
    //The idea being different shards live on different boxes
    protected final int shardsAtOnce;  
    
    private int[]                           randomSeq;
    private final Map<Integer,Integer>      offsetLookup = new HashMap<Integer,Integer>(CassandraUtils.maxDocsPerShard); //maps ids to offsets
    private final int                       reserveSlabSize = (int)Math.pow(2, 7);
    private final int                       offsetSlots     = CassandraUtils.maxDocsPerShard/reserveSlabSize;
    private final int                       expirationTime  = 60;  // seconds

    private final Map<String, LinkedBlockingQueue<IdInfo>> indexReserves   = new HashMap<String, LinkedBlockingQueue<IdInfo>>();
    private final Map<String, ShardInfo>    indexShards     = new HashMap<String, ShardInfo>();

    private static final Logger             logger          = Logger.getLogger(CassandraIndexManager.class);

    private class ShardInfo
    {
        public final String                     indexName;
        public final long                       ttl    = System.currentTimeMillis() + expirationTime * 1000 - 1000;
        public final TreeMap<Integer, NodeInfo> shards = new TreeMap<Integer, NodeInfo>();

        public ShardInfo(String indexName)
        {
            this.indexName = indexName;
        }
    }

    private class NodeInfo
    {
        public Integer              shard;
        public Map<String, Integer> nodes = new HashMap<String, Integer>();

        public NodeInfo(Integer shard)
        {
            this.shard = shard;
        }
    }

    private class IdInfo
    {
        public NodeInfo   node;
        public Integer    id;
        public Integer    offset;
        public final long ttl = System.currentTimeMillis() + expirationTime * 1000 - 1000;

        public IdInfo(NodeInfo node, Integer id, Integer offset)
        {
            this.node = node;
            this.id = id;
            this.offset = offset;
        }
    }

    public CassandraIndexManager(int shardsAtOnce)
    {
        this.shardsAtOnce = shardsAtOnce;

        logger.info("Shards at once: "+shardsAtOnce);
        
        // get our unique sequence
        Random r = new Random(getNodeSeed(getToken()));
        
        randomSeq = new int[offsetSlots];
        
        for(int i=0,offset=0; i<CassandraUtils.maxDocsPerShard; i++)
        {            
            if(i % reserveSlabSize == 0)
            {
                randomSeq[offset] = i;
                offset++;
            }               
        }
        
        randomSeq = shuffle(randomSeq, r);
        
        for(int i=0; i<offsetSlots; i++)
        {
            int start = randomSeq[i];
            for(int j=start; j<start+reserveSlabSize; j++){
                offsetLookup.put(j, i);
            }
        }
    }

    private ShardInfo getShardInfo(String indexName)
    {

        synchronized (indexName.intern())
        {          
            ShardInfo shards = indexShards.get(indexName);

            if (shards != null)
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

            ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, "shards".getBytes());
            
            ReadCommand cmd = new SliceFromReadCommand(CassandraUtils.keySpace, key, new ColumnParent(CassandraUtils.schemaInfoColumnFamily),
                    FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, false, 100);

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
                       
                        //goto each shard and get local offset
                        cmd = new SliceFromReadCommand(CassandraUtils.keySpace, 
                                CassandraUtils.hashKeyBytes((indexName+"~"+shardStr).getBytes(), CassandraUtils.delimeterBytes, "shards".getBytes()),
                                new ColumnParent(CassandraUtils.schemaInfoColumnFamily),
                                FBUtilities.EMPTY_BYTE_BUFFER, 
                                FBUtilities.EMPTY_BYTE_BUFFER, 
                                false, 100);
                       
                        List<Row> lrows = CassandraUtils.robustRead(ConsistencyLevel.QUORUM, cmd);

                        if (lrows != null || !lrows.isEmpty())
                        {
                            assert rows.size() == 1;

                            Row lrow = lrows.get(0);

                            if (lrow.cf != null && !lrow.cf.isMarkedForDelete())
                            {
                                for (IColumn lc : lrow.cf.getSortedColumns())
                                {
                                    NodeInfo nodes = new NodeInfo(shardNum);

                                    for (IColumn s : lc.getSubColumns())
                                    {
                                        String token = ByteBufferUtil.string(s.name());
                                        Integer offset = Integer.valueOf(ByteBufferUtil.string(s.value()));

                                        nodes.nodes.put(token, offset);
                                    }

                                    shards.shards.put(shardNum, nodes);
                                }
                            }                   
                        }
                    }
                }
            }

            indexShards.put(indexName, shards);

            logger.info(indexName + " has " + shards.shards.size() + " shards");

            return shards;
        }
    }

    public void deleteId(String indexName, long id)
    {

    }

    public long getMaxId(String indexName)
    {
        // find the max shard
        ShardInfo shards = getShardInfo(indexName);

        if(shards.shards.isEmpty())
            return 0;
        
        int highest = 0;
        
        //Find the highest *used* slab
        //loop is ordered by slab number
        for(Map.Entry<Integer, NodeInfo> e : shards.shards.entrySet())
        {    
            Integer currentOffset = null;
            for(Map.Entry<String, Integer> e1 : e.getValue().nodes.entrySet())
            {
                if(e1.getValue() > 0){           
                    currentOffset = e1.getValue();
                    break;
                }
            }
            
            if(currentOffset != null)
                highest = e.getKey();
        }
        
        return (CassandraUtils.maxDocsPerShard * highest);
    }

    public Long getId(String indexName, String key)
    {
        return checkForUpdate(indexName, key);
    }

    public Long checkForUpdate(String indexName, String key)
    {
        ByteBuffer keyCol = ByteBuffer.wrap(key.getBytes());
        ByteBuffer keyKey = CassandraUtils.hashKeyBytes((indexName + "~" + key).getBytes(), CassandraUtils.delimeterBytes, "keys".getBytes());

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
                    
                    if(subCols != null && !subCols.isEmpty())
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
        return StorageService.instance.getTokenMetadata().getToken(FBUtilities.getLocalAddress()).toString();
    }

    public long getNextId(String indexName, String key, RowMutation[] rowMutations)
    {
        if(rowMutations.length != 3)
            throw new IllegalArgumentException("rowMutations must be length 3");
        
        String myToken = getToken();
        ShardInfo shards = null;
        NodeInfo nodes[] = null;
        IdInfo idInfo    = null;
        
        int attempts = 0;
        while(attempts < reserveSlabSize){
            shards  = getShardInfo(indexName);
            nodes   = pickAShard(shards);

            idInfo = nextReservedId(indexName, nodes, myToken);
            
            if(idInfo == null){       
                attempts++;
                continue;
            }
            break;
        }
        
        if(idInfo == null)
            throw new IllegalStateException("Unable to reserve an id");
        
        ByteBuffer idCol  = ByteBuffer.wrap(String.valueOf(idInfo.id).getBytes());
        ByteBuffer keyCol = ByteBuffer.wrap(key.getBytes());

        // Permanently mark the id as taken
        ByteBuffer idKey = CassandraUtils.hashKeyBytes((indexName + "~" + idInfo.node.shard).getBytes(), CassandraUtils.delimeterBytes, "ids".getBytes());

        RowMutation rm = new RowMutation(CassandraUtils.keySpace, idKey);
        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, idCol, ByteBuffer.wrap(myToken.getBytes())),
                keyCol, System.currentTimeMillis());

        // Permanently link the key to the id
        ByteBuffer keyKey = CassandraUtils.hashKeyBytes((indexName+"~"+key).getBytes(), CassandraUtils.delimeterBytes, "keys".getBytes());
        Long val = new Long(idInfo.id + (idInfo.node.shard * CassandraUtils.maxDocsPerShard));
        ByteBuffer  idVal = ByteBuffer.wrap(val.toString().getBytes());

        RowMutation rm2 = new RowMutation(CassandraUtils.keySpace, keyKey);
        rm2.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, keyCol, idVal), FBUtilities.EMPTY_BYTE_BUFFER,
                System.currentTimeMillis());

        // Update last offset info for this shard
        RowMutation rm3 = updateNodeOffset(indexName+"~"+idInfo.node.shard, myToken, idInfo.node, idInfo.offset);
      
        rowMutations[0] = rm;
        rowMutations[1] = rm2;
        rowMutations[2] = rm3;
        
        return val;
    }
    
    public long getNextId(String indexName, String key)
    {      
      
        RowMutation[] rms = new RowMutation[3]; 
        
        Long val = getNextId(indexName, key, rms);
        
        //TODO: Delayed Insert!
        //Checks for more recent updates and disregards the older ones
        CassandraUtils.robustInsert(ConsistencyLevel.ONE, rms);
        
        return val;
    }

    public void resetCounter(String indexName)
    {
        // update all shards to 0 for all tokens
        ShardInfo shards = getShardInfo(indexName);

        List<RowMutation> rms = new ArrayList<RowMutation>();

        for (NodeInfo nodes : shards.shards.values())
        {
            for (String token : nodes.nodes.keySet()) 
                rms.add(updateNodeOffset(indexName+"~"+nodes.shard, token, nodes, randomSeq[0]));
        }

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rms.toArray(new RowMutation[] {}));
    }

    private IdInfo nextReservedId(String indexName, NodeInfo[] shards, String myToken)
    {
        if (logger.isDebugEnabled())
            logger.debug("in reserveIds for index " + indexName);

        synchronized (indexName.intern())
        {

            LinkedBlockingQueue<IdInfo> currentRsvpd = indexReserves.get(indexName);

            if (currentRsvpd != null)
            {
                // make sure none have timed out
                List<IdInfo> expired = null;

                for (IdInfo id : currentRsvpd)
                {
                    if (id.ttl <= System.currentTimeMillis())
                    {
                        if (expired == null)
                            expired = new ArrayList<IdInfo>();

                        expired.add(id);
                    }
                }

                if (expired != null)
                {
                    logger.info(expired.size() + " reserved ids for " + indexName + " have expired");
                    currentRsvpd.removeAll(expired);
                }
                if (!currentRsvpd.isEmpty()){
                    
                    //if(currentRsvpd.size() == 1)
                    //    logger.info("need more ids for "+myToken); 
                    
                    return currentRsvpd.poll();
                }
            }
            
            Map<NodeInfo, TreeSet<IdInfo>> rsvpdByNode = new LinkedHashMap<NodeInfo, TreeSet<IdInfo>>();

            for (NodeInfo node : shards)
            {

                // Order by offset
                TreeSet<IdInfo> rsvpd = new TreeSet<IdInfo>(new Comparator<IdInfo>() {

                    public int compare(IdInfo o1, IdInfo o2)
                    {
                        if (o1.offset == o2.offset)
                        //{
                        //    if(o1.node.shard)
                            return 0;

                        if (o1.offset < o2.offset)
                            return -1;

                        return 1;
                    }

                });

                Integer offset = node.nodes.get(myToken);                
                
                assert offset != null;              
                
                //goto next offset marker (unless its the first)
                if(offset != randomSeq[0])
                    offset = randomSeq[offsetLookup.get(offset)+1];
                                         
                if (offset > CassandraUtils.maxDocsPerShard)
                    throw new IllegalStateException("Invalid id marker found for shard: " + offset);

                ByteBuffer key = CassandraUtils.hashKeyBytes((indexName + "~" + node.shard).getBytes(), CassandraUtils.delimeterBytes, "ids".getBytes());

                // Write the reserves
                RowMutation rm = new RowMutation(CassandraUtils.keySpace, key);
             
                ByteBuffer id  = ByteBuffer.wrap(String.valueOf(offset).getBytes());
                ByteBuffer off = ByteBuffer.wrap(String.valueOf(offset).getBytes());

                rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, id, 
                        ByteBuffer.wrap(myToken.getBytes())), off, System.currentTimeMillis(), expirationTime);

                
                CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);

                // Read the columns back              
                IColumn supercol = null;
                int attempts = 0;
                while(supercol == null && attempts < 10){
                    List<Row> rows = CassandraUtils.robustRead(key, new QueryPath(CassandraUtils.schemaInfoColumnFamily),
                        Arrays.asList(id), ConsistencyLevel.QUORUM);

                    if (rows == null || rows.size() == 0)
                    {
                        continue;
                    }

                    if(rows.size() == 1){
                        Row row = rows.get(0);

                        if (row.cf == null || row.cf.isMarkedForDelete())
                        {
                            continue;
                        }
                    
               
                        supercol = rows.get(0).cf.getColumn(id);
                    }
                    attempts ++;
                }
                
                if (supercol == null)
                    throw new IllegalStateException("just wrote "+offset+", but didn't read it");

               
                long minTtl = Long.MAX_VALUE;
                ByteBuffer winningToken = null;
                
                // See which ones we successfully reserved
                for (IColumn c : supercol.getSubColumns())
                {
                                              
                    // someone already took this id
                    if (!(c instanceof ExpiringColumn) && !(c instanceof DeletedColumn))
                    {
                         if(logger.isDebugEnabled())
                             logger.debug(offset+" was taken by "+ByteBufferUtil.string(c.name()));
                            
                         winningToken = null;
                         break;
                     }

                     // expired reservation
                     if (c.isMarkedForDelete())
                          continue;

                     if( c.timestamp() == minTtl && winningToken.compareTo(c.name()) <= 0 )
                     {
                         winningToken = c.name();
                     }
                            
                     if (c.timestamp() < minTtl)
                     {
                         minTtl = c.timestamp();
                         winningToken = c.name();
                     }
                }

                // we won!
                if (winningToken != null && ByteBufferUtil.string(winningToken).equals(myToken))
                {                    
                    for(int i=offset; i == offset || i % reserveSlabSize != 0; i++)
                    {                   
                        rsvpd.add(new IdInfo(node, i, i));
                    }
                } 
                else
                {
                    //secial case, otherwise we never move on
                    if(offset == randomSeq[0])
                        offset+=1;
                    
                    updateNodeOffset(indexName+"~"+node.shard, myToken, node, offset);
                }

                rsvpdByNode.put(node, rsvpd);

                if (logger.isDebugEnabled())
                   logger.debug("offset for shard " + node.shard + " " + offset);
            }

            currentRsvpd = interleaveByNode(rsvpdByNode);

            indexReserves.put(indexName, currentRsvpd);

            if (logger.isDebugEnabled())
                logger.debug("Reserved " + currentRsvpd.size() + " ids for "+myToken);

            return currentRsvpd.poll();
        }
    }

    private LinkedBlockingQueue<IdInfo> interleaveByNode(Map<NodeInfo, TreeSet<IdInfo>> rsvpdByNode)
    {
        LinkedBlockingQueue<IdInfo> rsvpd = new LinkedBlockingQueue<IdInfo>();

        while (true)
        {

            boolean allEmpty = true;

            // take one from each shard (till none left)
            for (Map.Entry<NodeInfo, TreeSet<IdInfo>> entry : rsvpdByNode.entrySet())
            {
                TreeSet<IdInfo> ids = entry.getValue();

                if (ids.isEmpty())
                    continue;

                rsvpd.add(ids.first());
                ids.remove(ids.first());
                allEmpty = false;
            }

            if (allEmpty)
                return rsvpd;
        }
    }

    /**
     * Looks for appropriate shard to reserve ids from
     * 
     * TODO: calculate where other tokens are in their sequence
     * 
     * @param shards
     * @return
     */
    private NodeInfo[] pickAShard(ShardInfo shards)
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

                Integer offset = nodes.nodes.get(myToken);

                //new shard for this node
                if (offset == null)
                {
                    // this means shard was started by another node
                    offset = randomSeq[0];
                    
                    updateNodeOffset(shards.indexName+"~"+nodes.shard, myToken, nodes, offset);
                }

                //can we still use this shard?
                if (offsetLookup.get(offset)+1 < offsetSlots )
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

    private NodeInfo addNewShard(String indexName)
    {
        ShardInfo shards = getShardInfo(indexName);

        // get max shard
        Integer maxShard = -1;
        if (!shards.shards.isEmpty())
            maxShard = shards.shards.lastKey();

        NodeInfo nodes = new NodeInfo(maxShard + 1);

        RowMutation rm  = updateNodeOffset(indexName, getToken(), nodes, randomSeq[0]); // offset 0
        RowMutation rm2 = updateNodeOffset(indexName + "~" + nodes.shard, getToken(), nodes, 0); // offset 0

        
        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm, rm2);

        shards.shards.put(maxShard + 1, nodes);

        logger.info("added new shard for " + indexName + " " + (maxShard + 1));

        return nodes;
    }

    private RowMutation updateNodeOffset(String indexName, String myToken, NodeInfo node, Integer offset)
    {
        // Update last offset info for this shard
        ByteBuffer shardKey = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, "shards".getBytes());
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, shardKey);

        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, 
                             ByteBuffer.wrap(String.valueOf(node.shard).getBytes()), 
                             ByteBuffer.wrap(myToken.getBytes())), 
               ByteBuffer.wrap(String.valueOf(offset).getBytes()),
               System.currentTimeMillis());

        // update locally
        node.nodes.put(myToken, offset);

        if (logger.isDebugEnabled())
            logger.debug("updated node offset for " + indexName + "(" + node.shard + ")(" + myToken + ") to " + offset);

        return rm;
    }

    private int getNodeSeed(String token)
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

        md.update(token.getBytes());

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
        //Always place maxVal at end (to avoid NPE)

        // i is the number of items remaining to be shuffled.
        for (int i = array.length-1; i > 1; i--)
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

    public static int getShardFromDocId(long docId){
        return (int) Math.floor(docId / CassandraUtils.maxDocsPerShard);
    }

    public static int getShardedDocId(long docId){
        return (int) docId % CassandraUtils.maxDocsPerShard;
    }
}
