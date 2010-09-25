package lucandra.cluster;

import java.io.IOException;
import java.util.List;

import lucandra.CircuitBreaker;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Based on contributed cassandra zk lock mechanism
 * 
 */
public class ZkIndexManager extends AbstractIndexManager implements Watcher {

    private static final Logger logger = Logger.getLogger(ZkIndexManager.class);

    // this must include hyphen (-) as the last character. substring search
    // relies on it
    private final String LockPrefix = "i-";

    private final CircuitBreaker breaker = new CircuitBreaker(3, 3);

    // how long to sleep between retries. Actual time slept is RetryInterval
    // multiplied by how
    // many times have we already tried.
    private final long RetryInterval = 500L;

    // Session timeout to ZooKeeper
    private final int SessionTimeout = 3000;

    private long lastConnect = 0;

    private ZooKeeper zk = null;
    private String root = "/Ctrs";

    private String hostString = new String();
    private String counterPath;
    private String lockPath;

    private Integer mutex = null;

    public ZkIndexManager(List<String> zooKeepers, String zkPort) {
        super();

        mutex = new Integer(1);
         for (String zooKeeper : zooKeepers) {
            hostString += (hostString.isEmpty()) ? "" : ",";
            hostString += zooKeeper + ":" + zkPort;
        }

        while (true) {
            try {
                connectZooKeeper();
                if (!root.isEmpty() && zk.exists(root, false) == null) {
                    logger.info("Mutex root " + root + " does not exists, creating");

                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    
                }
                return;
            } catch (KeeperException e) {
                breaker.failure();
            } catch (InterruptedException e) {
               breaker.failure();
            }
        }

    }

    /**
     * Connect to zookeeper server
     */
    private synchronized void connectZooKeeper() {
        if (zk != null && zk.getState() != ZooKeeper.States.CLOSED)
            return;

        try {
            logger.info("Connecting to ZooKeepers: " + hostString);
            zk = new ZooKeeper(hostString, SessionTimeout, this);
            breaker.success();

        } catch (IOException e) {
            breaker.failure();
            logger.info("Zookeeper connection failed: ", e);
        }

    }

    /**
     * close current session and try to connect to zookeeper server
     */
    private synchronized void reestablishZooKeeperSession() {
        long now = System.currentTimeMillis();

        // let's not flood zookeeper with connection requests
        if (!breaker.allow()) {
            try {
                Thread.sleep(RetryInterval);
            } catch (InterruptedException ignore) {
                // Just fall through to retry
            }

            if (logger.isTraceEnabled())
                logger.trace("Only " + (now - lastConnect) + "ms passed since last reconnect, not trying again yet");
            return;
        }

        lastConnect = now;

        try {
            zk.close();
        } catch (Exception e) {
            // ignore all exceptions. we're calling this just to make sure
            // ephemeral nodes are
            // deleted. zk might be in an inconsistent state and cause
            // exception.
        }

        connectZooKeeper();
    }

    /**
     * process any events from ZooKeeper. We simply wake up any clients that are
     * waiting for file deletion. Number of clients is usually very small (most
     * likely just one), so no need for any complex logic.
     */
    public void process(WatchedEvent event) {
        if (logger.isTraceEnabled())
            logger.trace("Got event " + event.getType() + ", keeper state " + event.getState() + ", path " + event.getPath());

        synchronized (mutex) {
            mutex.notifyAll();
        }
    }

    private boolean isConnected() {
        return zk.getState() == ZooKeeper.States.CONNECTED;
    }

   

    @Override
    public int incrementDocId(String indexName)  {
       
        
        while(true){
        
            try {
                
                String node = null;
                
                try {
                    node = zk.create(root+"/"+indexName + "/" + LockPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                } catch (NoNodeException e) {
                    logger.info(lockPath + " does not exist, creating");
                    zk.create(root+"/"+indexName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                   
                    node = zk.create(root+"/"+ indexName + "/" + LockPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                }
                
                return Integer.parseInt(node.substring(node.lastIndexOf('-')+1));
                
            } catch (NoNodeException e) {
                throw new IllegalStateException(e);
                
            } catch (KeeperException e) {
                breaker.failure();
                
                if (!isConnected())
                    reestablishZooKeeperSession();

            } catch (InterruptedException e) {
                breaker.failure();
                
                if (!isConnected())
                    reestablishZooKeeperSession();

            }
        }
    }

    @Override
    public int getCurrentDocId(String indexName) {
        while(true){
            
            try {
                Stat node = zk.exists(root+"/"+indexName, false);

                return node.getNumChildren();
                
            } catch (NoNodeException e) {
                return 0;
            } catch (KeeperException e) {
                breaker.failure();
                
                if (!isConnected())
                    reestablishZooKeeperSession();

            } catch (InterruptedException e) {
                breaker.failure();
                
                if (!isConnected())
                    reestablishZooKeeperSession();

            }
        }
    }

    

}
