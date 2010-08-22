package lucandra.cluster;

import lucandra.CassandraUtils;


public abstract class AbstractIndexManager {
     
    public abstract int incrementDocId(String indexName);
  
    public abstract int getCurrentDocId(String indexName);
    
    //abstract int deleteDocId(int docId);
    
    public static int getShardFromDocId(int docId){
        return (int) Math.floor(docId / CassandraUtils.maxDocsPerShard);
    }

}
