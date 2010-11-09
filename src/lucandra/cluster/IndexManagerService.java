package lucandra.cluster;


public class IndexManagerService
{
    public static final AbstractIndexManager instance;
    public static final Integer              shardsAtOnce = Integer.valueOf(System.getProperty("shards.at.once", "4"));

    static
    {   
        instance = new CassandraIndexManager(shardsAtOnce);
    }

}
