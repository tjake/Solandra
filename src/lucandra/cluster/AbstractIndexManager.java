package lucandra.cluster;

import lucandra.CassandraUtils;


public abstract class AbstractIndexManager {
    
    //To increase throughput we distribute docs across a number of shards at once
    //The idea being different shards live of different boxes
    private final int shardsAtOnce;  
    
    public AbstractIndexManager(int shardsAtOnce){
        this.shardsAtOnce = shardsAtOnce;
    }
    
    public long incrementDocId(String indexName){
        long id = internalIncrement(indexName);

        //calculate shard from info
        long shard = id % shardsAtOnce;
        long actualShard = (long)Math.floor(id / (CassandraUtils.maxDocsPerShard*shardsAtOnce));
        long shardDoc  = (long) id % (CassandraUtils.maxDocsPerShard*shardsAtOnce);
        
        return ((actualShard + shard) * CassandraUtils.maxDocsPerShard) + (shardDoc/shardsAtOnce);

    }
  
    public long getCurrentDocId(String indexName){
        long id = internalFetch(indexName);
    
        //calculate shard from info
        long shard = shardsAtOnce-1; //always return max shard offset
        long actualShard = (long)Math.floor(id / (CassandraUtils.maxDocsPerShard*shardsAtOnce));
        long shardDoc  = (long) id % (CassandraUtils.maxDocsPerShard*shardsAtOnce);
        
        return ((actualShard + shard)  * CassandraUtils.maxDocsPerShard) + shardDoc/shardsAtOnce;

    }
    
     
    public abstract long internalIncrement(String indexName);
    
    public abstract long internalFetch(String indexName);
    
    public static int getShardFromDocId(long docId){
        return (int) Math.floor(docId / CassandraUtils.maxDocsPerShard);
    }

    public static int getShardedDocId(long docId){
        return (int) docId % CassandraUtils.maxDocsPerShard;
    }
   
    
}
