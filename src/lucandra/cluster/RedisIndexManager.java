package lucandra.cluster;

import java.util.concurrent.TimeoutException;

import redis.clients.jedis.JedisPool;

public class RedisIndexManager extends AbstractIndexManager {

    private JedisPool redisService;
    private static final String root = "/Ctrs";
    
    
    public RedisIndexManager(JedisPool redisService, int shardsAtOnce){
        super(shardsAtOnce);
        this.redisService = redisService;
        
    }
    
   
    @Override
    public long internalIncrement(String indexName) {
        long id;
        String key = root + "/" + indexName;
     
            try
            {
                id = redisService.getResource(200).incr(key);
            }
            catch (TimeoutException e)
            {
                throw new RuntimeException(e);
            }
        
        return id-1;  
    }


    @Override
    public long internalFetch(String indexName) {
        long id;
        String key = root + "/" + indexName;
        try {
            id = redisService.getResource(200).incrBy(key, 0);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        
        return id-1;
    }


    @Override
    public void resetCounter(String indexName) {      
        String key = root + "/" + indexName;
        try {
            redisService.getResource(200).set(key, "0");
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }    
    }

}
