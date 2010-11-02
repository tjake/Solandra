package lucandra.cluster;


public class IndexManagerService
{
    public static final AbstractIndexManager indexManager;

    public static final Integer              shardsAtOnce = Integer.valueOf(System.getProperty("shads.at.once", "4"));

    static
    {   
        indexManager = new CassandraIndexManager(shardsAtOnce, 0.9);
    }

}
