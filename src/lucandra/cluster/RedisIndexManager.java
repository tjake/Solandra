package lucandra.cluster;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisService;

public class RedisIndexManager extends AbstractIndexManager {

    private JRedisService redisService;
    private static final String root = "/Ctrs";
    
    
    public RedisIndexManager(JRedisService redisService, int shardsAtOnce){
        super(shardsAtOnce);
        this.redisService = redisService;
        
    }
    
   
    @Override
    public long internalIncrement(String indexName) {
        long id;
        String key = root + "/" + indexName;
        try {
            id = redisService.incr(key);
        } catch (RedisException e) {
            throw new RuntimeException(e);
        }
        
        return id-1;  
    }


    @Override
    public long internalFetch(String indexName) {
        long id;
        String key = root + "/" + indexName;
        try {
            id = redisService.incrby(key, 0);
        } catch (RedisException e) {
            throw new RuntimeException(e);
        }
        
        return id-1;
    }


    @Override
    public void resetCounter(String indexName) {      
        String key = root + "/" + indexName;
        try {
            redisService.set(key, 0);
        } catch (RedisException e) {
            throw new RuntimeException(e);
        }    
    }

}
