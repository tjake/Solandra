package lucandra.cluster;

import lucandra.CassandraUtils;


public abstract class AbstractIndexManager {
    
    
    public abstract void resetCounter(String indexName);
     
    /**
     * Will get the next unused id available
     * 
     * There is no ordering guarantees here. just uniqueness of the id.
     * 
     * *NOTE* this does not check if the key is already used.
     * If you want to check this see getId()
     * 
     * @param indexName
     * @param key
     * @return
     */
    public abstract long getNextId(String indexName, String key);
    
    public abstract long getMaxId(String indexName);
    
    public abstract void deleteId(String indexName, long id);
    
    public abstract Long getId(String indexName, String key);    
    
    public static int getShardFromDocId(long docId){
        return (int) Math.floor(docId / CassandraUtils.maxDocsPerShard);
    }

    public static int getShardedDocId(long docId){
        return (int) docId % CassandraUtils.maxDocsPerShard;
    }
   
    
}
