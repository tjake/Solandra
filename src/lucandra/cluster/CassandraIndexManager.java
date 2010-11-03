package lucandra.cluster;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import lucandra.CassandraUtils;

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

import com.google.common.collect.MapMaker;

public class CassandraIndexManager extends AbstractIndexManager
{
    private final int[]                               randomSeq;
    private final double                              collisionThreshold;
    private final int                                 reserveSlabSize = 100;
    private final int                                 expirationTime  = 120;                                          // seconds

    private final ConcurrentMap<String, List<IdInfo>> indexReserves   = new MapMaker().expiration(expirationTime,
                                                                              TimeUnit.SECONDS).makeMap();
    private final ConcurrentMap<String, ShardInfo>    indexShards     = new MapMaker().expiration(expirationTime,
                                                                              TimeUnit.SECONDS).makeMap();

    private static final Logger                       logger          = Logger.getLogger(CassandraIndexManager.class);

    private class ShardInfo
    {
        public String                     indexName;
        public TreeMap<Integer, NodeInfo> shards = new TreeMap<Integer, NodeInfo>();

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
        public NodeInfo node;
        public Integer  id;
        public Integer  offset;

        public IdInfo(NodeInfo node, Integer id, Integer offset)
        {
            this.node = node;
            this.id = id;
            this.offset = offset;
        }
    }

    public CassandraIndexManager(int shardsAtOnce, double collisionThreshold)
    {
        super(shardsAtOnce);

        assert (collisionThreshold >= 0.0 && collisionThreshold <= 1.0);

        this.collisionThreshold = collisionThreshold;

        // get our unique sequence
        Random r = new Random(getNodeSeed(getToken()));
        randomSeq = shuffle(CassandraUtils.maxDocsPerShard, r);
    }

    private ShardInfo getShardInfo(String indexName)
    {
        ShardInfo shards = indexShards.get(indexName);

        if (shards != null)
            return shards;

        ReadCommand cmd = new SliceFromReadCommand(CassandraUtils.keySpace, ByteBuffer.wrap((indexName + "/shards")
                .getBytes()), new ColumnParent(CassandraUtils.schemaInfoColumnFamily), FBUtilities.EMPTY_BYTE_BUFFER,
                FBUtilities.EMPTY_BYTE_BUFFER, false, 100);

        List<Row> rows = CassandraUtils.robustRead(ConsistencyLevel.QUORUM, cmd);

        shards = new ShardInfo(indexName);
        if (rows != null || !rows.isEmpty())
        {
            assert rows.size() == 1;

            Row row = rows.get(0);

            if (row.cf != null && !row.cf.isMarkedForDelete())
            {

                assert row.cf.getSortedColumns() != null;

                // Each column represents each shard and latest id for each node
                // {"shard1" : {"node1" : 1234}}
                for (IColumn c : row.cf.getSortedColumns())
                {
                    String shardStr = ByteBufferUtil.string(c.name());
                    Integer shardNum = Integer.valueOf(shardStr);

                    NodeInfo nodes = new NodeInfo(shardNum);

                    for (IColumn s : c.getSubColumns())
                    {
                        String token = ByteBufferUtil.string(s.name());
                        Integer offset = Integer.valueOf(ByteBufferUtil.string(s.value()));

                        nodes.nodes.put(token, offset);
                    }

                    shards.shards.put(shardNum, nodes);
                }
            }
        }

        indexShards.put(indexName, shards);

        return shards;
    }

    public void deleteId(String indexName, long id)
    {

    }

    public long internalFetch(String indexName)
    {
        // find the max shard
        ShardInfo shards = getShardInfo(indexName);

        int highest = shards.shards.lastKey();

        return (CassandraUtils.maxDocsPerShard * highest);
    }

    public Long internalFetch(String indexName, String key)
    {
        return null;
    }

    public Long checkForUpdate(String indexName, String key)
    {
        ByteBuffer keyCol = ByteBuffer.wrap(key.getBytes());

        // First, be sure this isn't an update
        ByteBuffer keyKey = ByteBuffer.wrap((indexName + "/keys").getBytes());

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
                    ByteBuffer idVal = col.getSubColumns().iterator().next().name();
                    Long id = Long.valueOf(ByteBufferUtil.string(idVal));
                    return id;
                }
            }
        }

        return null;
    }

    public String getToken()
    {
        return StorageService.instance.getTokenMetadata().getToken(FBUtilities.getLocalAddress()).toString();
    }

    public long internalIncrement(String indexName, String key)
    {
        Long updateId = checkForUpdate(indexName, key);

        if (updateId != null)
            return -updateId; // dirty, dirty

        ShardInfo shards = getShardInfo(indexName);
        NodeInfo nodes[] = pickAShard(shards);
        String myToken = getToken();

        List<IdInfo> ids = reserveIds(indexName, nodes, myToken);

        IdInfo idInfo = ids.remove(0);
        ByteBuffer idCol = ByteBuffer.wrap(String.valueOf(idInfo.id).getBytes());

        ByteBuffer keyCol = ByteBuffer.wrap(key.getBytes());

        // Permanently mark the id as taken
        ByteBuffer idKey = ByteBuffer.wrap((indexName + "~" + idInfo.node.shard + "/ids").getBytes());

        RowMutation rm = new RowMutation(CassandraUtils.keySpace, idKey);
        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, idCol, ByteBuffer.wrap(myToken.getBytes())),
                keyCol, System.currentTimeMillis());

        // Permanently link the key to the id
        // TODO: secondary index?
        ByteBuffer keyKey = ByteBuffer.wrap((indexName + "/keys").getBytes());
        Long val = new Long(idInfo.id + (idInfo.node.shard * CassandraUtils.maxDocsPerShard));
        ByteBuffer idVal = ByteBuffer.wrap(val.toString().getBytes());

        RowMutation rm2 = new RowMutation(CassandraUtils.keySpace, keyKey);
        rm2.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, keyCol, idVal), FBUtilities.EMPTY_BYTE_BUFFER,
                System.currentTimeMillis());

        // Update last offset info for this shard
        RowMutation rm3 = updateNodeOffset(indexName, getToken(), idInfo.node, idInfo.offset);
        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm, rm2, rm3);

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
                rms.add(updateNodeOffset(indexName, token, nodes, 0));
        }

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rms.toArray(new RowMutation[] {}));
    }

    private List<IdInfo> reserveIds(String indexName, NodeInfo[] shards, String myToken)
    {
        if (logger.isDebugEnabled())
            logger.debug("in reserveIds for index " + indexName);

        synchronized (indexName.intern())
        {

            List<IdInfo> currentRsvpd = indexReserves.get(indexName);

            if (currentRsvpd != null && currentRsvpd.size() > 0)
                return currentRsvpd;

            Map<NodeInfo, TreeSet<IdInfo>> rsvpdByNode = new LinkedHashMap<NodeInfo, TreeSet<IdInfo>>();

            for (NodeInfo node : shards)
            {

                // Order by offset
                TreeSet<IdInfo> rsvpd = new TreeSet<IdInfo>(new Comparator<IdInfo>() {

                    public int compare(IdInfo o1, IdInfo o2)
                    {
                        if (o1.offset == o2.offset)
                            return 0;

                        if (o1.offset < o2.offset)
                            return -1;

                        return 1;
                    }

                });

                Integer offset = node.nodes.get(myToken);

                assert offset != null;

                if (offset > randomSeq.length)
                    throw new IllegalStateException("Invalid id marker found for shard: " + offset);

                ByteBuffer key = ByteBuffer.wrap((indexName + "~" + node.shard + "/ids").getBytes());

                // Write the reserves
                RowMutation rm = new RowMutation(CassandraUtils.keySpace, key);

                List<ByteBuffer> ids = new ArrayList<ByteBuffer>(reserveSlabSize);

                for (int i = offset + 1; i < (offset + reserveSlabSize + 1); i++)
                {
                    ByteBuffer id = ByteBuffer.wrap(String.valueOf(randomSeq[i]).getBytes());
                    ByteBuffer off = ByteBuffer.wrap(String.valueOf(i).getBytes());

                    rm.add(
                            new QueryPath(CassandraUtils.schemaInfoColumnFamily, id, ByteBuffer
                                    .wrap(myToken.getBytes())), off, 0, expirationTime);

                    ids.add(id);
                }

                CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);

                // Read the columns back
                List<Row> rows = CassandraUtils.robustRead(key, new QueryPath(CassandraUtils.schemaInfoColumnFamily),
                        ids, ConsistencyLevel.QUORUM);

                // See which ones we successfully reserved
                if (rows == null || rows.size() == 0)
                {
                    logger.info("read back no rows");
                    continue;
                }

                assert rows.size() == 1;
                Row row = rows.get(0);

                if (row.cf == null || row.cf.isMarkedForDelete())
                {
                    continue;
                }

                Collection<IColumn> supercols = rows.get(0).cf.getSortedColumns();

                if (supercols.size() != ids.size())
                    throw new IllegalStateException("wrote " + ids.size() + " ids, but read " + supercols.size());

                for (IColumn sc : supercols)
                {
                    Integer id = Integer.valueOf(ByteBufferUtil.string(sc.name()));
                    Integer off = null;
                    int minTtl = Integer.MAX_VALUE;
                    String winningToken = "";

                    for (IColumn c : sc.getSubColumns())
                    {
                        // someone already took this id
                        if (!(c instanceof ExpiringColumn))
                        {
                            winningToken = "";
                            break;
                        }

                        // expired
                        if (c.isMarkedForDelete())
                            continue;

                        if (c.getLocalDeletionTime() < minTtl)
                        {
                            minTtl = c.getLocalDeletionTime();
                            winningToken = ByteBufferUtil.string(c.name());
                            off = Integer.valueOf(ByteBufferUtil.string(c.value()));
                        }
                    }

                    // we won!
                    if (winningToken.equals(myToken))
                    {
                        rsvpd.add(new IdInfo(node, id, off));
                    }
                }

                rsvpdByNode.put(node, rsvpd);

                if (logger.isDebugEnabled())
                    logger.debug("offset for shard " + node.shard + " " + offset);
            }

            currentRsvpd = interleaveByNode(rsvpdByNode);

            indexReserves.put(indexName, currentRsvpd);

            if (logger.isDebugEnabled())
                logger.debug("Reserved " + currentRsvpd.size() + "ids");

            return currentRsvpd;
        }
    }

    private List<IdInfo> interleaveByNode(Map<NodeInfo, TreeSet<IdInfo>> rsvpdByNode)
    {
        List<IdInfo> rsvpd = Collections.synchronizedList( new LinkedList<IdInfo>() );

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
        String myToken = getToken();

        NodeInfo[] picked = new NodeInfo[shardsAtOnce];

        int maxShard = -1;
        int pickedShard = 0;

        for (Map.Entry<Integer, NodeInfo> shard : shards.shards.entrySet())
        {
            NodeInfo nodes = shard.getValue();

            Integer offset = nodes.nodes.get(myToken);

            if (offset == null)
            {
                // this means shard was started by another node
                updateNodeOffset(shards.indexName, myToken, nodes, 0);
            }

            else if (offset + reserveSlabSize < randomSeq.length)
            {
                // if we still have
                if (((1 - (offset / randomSeq.length)) * randomSeq.length) > reserveSlabSize)
                {
                    picked[pickedShard] = nodes;
                    pickedShard++;
                    if (pickedShard >= shardsAtOnce)
                        return picked;
                }
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

    private NodeInfo addNewShard(String indexName)
    {
        ShardInfo shards = getShardInfo(indexName);

        // get max shard
        Integer maxShard = -1;
        if (!shards.shards.isEmpty())
            maxShard = shards.shards.lastKey();

        NodeInfo nodes = new NodeInfo(maxShard + 1);

        RowMutation rm = updateNodeOffset(indexName, getToken(), nodes, 0); // offset
                                                                            // 0

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);

        shards.shards.put(maxShard + 1, nodes);

        logger.info("added new shard for " + indexName + " " + (maxShard + 1));

        return nodes;
    }

    private RowMutation updateNodeOffset(String indexName, String myToken, NodeInfo node, Integer offset)
    {
        // Update last offset info for this shard
        ByteBuffer shardKey = ByteBuffer.wrap((indexName + "/shards").getBytes());
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, shardKey);

        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, ByteBuffer.wrap(String.valueOf(node.shard)
                .getBytes()), ByteBuffer.wrap(myToken.getBytes())), ByteBuffer.wrap(String.valueOf(offset).getBytes()),
                0);

        // update locally
        node.nodes.put(myToken, offset);

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
    private int[] shuffle(int max, Random rng)
    {

        // fill
        int[] array = new int[max];
        for (int i = 0; i < max; i++)
        {
            array[i] = i;
        }

        // i is the number of items remaining to be shuffled.
        for (int i = max; i > 1; i--)
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

}
