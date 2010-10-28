package lucandra.cluster;

import redis.clients.jedis.JedisPool;

public class IndexManagerService
{

    public static final JedisPool            pool;
    public static final AbstractIndexManager indexManager;

    public static final Integer              shardsAtOnce = Integer.valueOf(System.getProperty("shads.at.once", "4"));

    static
    {

       
        pool = new JedisPool(System.getProperty("redis.host", "localhost"),
                Integer.valueOf(System.getProperty("redis.port", "6379")), 2000);
        pool.setResourcesNumber(10);
        pool.init();
       
        indexManager = new RedisIndexManager(pool, shardsAtOnce);
    }

}
