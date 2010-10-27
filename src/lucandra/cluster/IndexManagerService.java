package lucandra.cluster;

import org.jredis.connector.ConnectionSpec;
import org.jredis.ri.alphazero.JRedisService;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;

public class IndexManagerService
{

    public static final JRedisService        service;
    public static final AbstractIndexManager indexManager;

    public static final Integer              shardsAtOnce = Integer.valueOf(System.getProperty("shads.at.once", "4"));

    static
    {

        int database = 11;
        int connCnt = 7;

        ConnectionSpec connectionSpec = DefaultConnectionSpec.newSpec(System.getProperty("redis.host", "localhost"),
                Integer.valueOf(System.getProperty("redis.port", "6379")), database, "jredis".getBytes());

        service = new JRedisService(connectionSpec, connCnt);
        indexManager = new RedisIndexManager(service, shardsAtOnce);
    }

}
