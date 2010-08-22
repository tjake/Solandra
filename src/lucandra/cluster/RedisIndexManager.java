package lucandra.cluster;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisService;

public class RedisIndexManager extends AbstractIndexManager {

    private JRedisService redisService;
    private static final String root = "/Ctrs";
    
    
    public RedisIndexManager(JRedisService redisService){
        super();
        this.redisService = redisService;
        
    }
    
   
    @Override
    public int incrementDocId(String indexName) {
        int id;
        String key = root + "/" + indexName;
        try {
            id = (int) redisService.incr(key);
        } catch (RedisException e) {
            throw new RuntimeException(e);
        }
        
        return id;  
    }


    @Override
    public int getCurrentDocId(String indexName) {
        int id;
        String key = root + "/" + indexName;
        try {
            id = (int) redisService.incrby(key, 0);
        } catch (RedisException e) {
            throw new RuntimeException(e);
        }
        
        return id;
    }

}
