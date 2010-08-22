package lucandra.cluster;

import java.io.IOException;
import java.util.List;

import lucandra.CassandraUtils;
import lucandra.CircuitBreaker;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Based on contributed cassandra zk lock mechanism
 * 
 */
public class ZkIndexManager extends AbstractIndexManager implements Watcher {

    private static final Logger logger = Logger.getLogger(ZkIndexManager.class);

    // this must include hyphen (-) as the last character. substring search
    // relies on it
    private final String LockPrefix = "lock-";

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
        //counterPath    = root + "/__" + name;       
        //lockPath       = root + "/" + name;
             
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

    /**
     * lock
     * 
     * @param lockName
     *            lock to be locked for writing. name can be any string, but it
     *            must not include slash (/) or any character disallowed by
     *            ZooKeeper (see
     *            hadoop.apache.org/zookeeper/docs/current/zookeeperProgrammers
     *            .html#ch_zkDataModel).
     * @return name of the znode inside zookeeper holding this lock.
     * @throws InterruptedException
     * @throws KeeperException
     */
    private String lock(String indexName) {

        return lockInternal(indexName);

    }

    /**
     * creates lock znode in zookeeper under lockPath. Lock name is LockPrefix
     * plus ephemeral sequence number given by zookeeper
     * 
     * @param lockPath
     *            name of the lock (directory in zookeeper)
     */
    private String createLockZNode() throws KeeperException, InterruptedException {
        String lockZNode = null;

        try {
            lockZNode = zk.create(lockPath + "/" + LockPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (NoNodeException e) {
            logger.info(lockPath + " does not exist, creating");
            zk.create(lockPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
           
            lockZNode = zk.create(lockPath + "/" + LockPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        
        

        return lockZNode;
    }

    /**
     * lockInteral does the actual locking.
     * 
     * @param same
     *            as in lock
     */
    private String lockInternal(String lockName) {
        String lockZNode = null;

        while (true) {

            try {

                if (!isConnected())
                    reestablishZooKeeperSession();

                lockZNode = createLockZNode();

                if (logger.isTraceEnabled())
                    logger.trace("lockZNode created " + lockZNode);

                while (true) {
                    // check what is our ID (sequence number at the end of file
                    // name added by ZK)
                    int mySeqNum = Integer.parseInt(lockZNode.substring(lockZNode.lastIndexOf('-') + 1));
                    int previousSeqNum = -1;
                    String predessor = null;

                    // get all children of lock znode and find the one that is
                    // just before us, if
                    // any. This must be inside loop, as children might get
                    // deleted out of order because
                    // of client disconnects. We cannot assume that the file
                    // that is in front of us this
                    // time, is there next time. It might have been deleted even
                    // though earlier files
                    // are still there.
                    List<String> children = zk.getChildren(lockPath, false);
                    if (children.isEmpty()) {
                        logger.warn("No children in " + lockPath + " although one was just created. Going to try again");
                        lockZNode = createLockZNode();
                        continue;
                    }
                    for (String child : children) {
                        if (logger.isTraceEnabled())
                            logger.trace("child: " + child);
                        int otherSeqNum = Integer.parseInt(child.substring(child.lastIndexOf('-') + 1));
                        if (otherSeqNum < mySeqNum && otherSeqNum > previousSeqNum) {
                            previousSeqNum = otherSeqNum;
                            predessor = child;
                        }
                    }

                    // our sequence number is smallest, we have the lock
                    if (previousSeqNum == -1) {
                        if (logger.isTraceEnabled())
                            logger.trace("No smaller znode sequences, " + lockZNode + " acquired lock");
                        return lockZNode;
                    }

                    // there is at least one znode before us. wait for it to be
                    // deleted.
                    synchronized (mutex) {
                        if (zk.exists(lockPath + "/" + predessor, true) == null) {
                            if (logger.isTraceEnabled())
                                logger.trace(predessor + " does not exists, " + lockZNode + " acquired lock");
                            break;
                        } else if (logger.isTraceEnabled())
                            logger.trace(predessor + " is still here, " + lockZNode + " must wait");

                        mutex.wait();

                        if (isConnected() == false) {
                            logger.info("ZooKeeper disconnected while waiting for lock");
                            throw new KeeperException.ConnectionLossException();
                        }
                    }
                }

            } catch (InterruptedException e) {
                breaker.failure();
            } catch (KeeperException e) {
                breaker.failure();
            }

            return lockZNode;
        }
    }

    /**
     * unlock
     * 
     * @param lockZNode
     *            this MUST be the string returned by lock call. Otherwise there
     *            will be chaos.
     */
    public void unlock(String lockZNode) {
        assert (lockZNode != null);

        if (logger.isTraceEnabled())
            logger.trace("deleting " + lockZNode);

        try {
            zk.delete(lockZNode, -1);
        } catch (Exception e) {
            // We do not do anything here. The idea is to check that everything
            // goes OK when
            // locking and let unlock always succeed from client's point of
            // view. Ephemeral
            // nodes should be taken care of by ZooKeeper, so ignoring any
            // errors here should
            // not break anything.
        }
    }


    @Override
    public int incrementDocId(String indexName)  {
       
        String lockName = null;

        while (true) {

            try {
                lockName = lock(indexName);

                byte[] bval = zk.getData(counterPath, false, null);

                int val = CassandraUtils.byteArrayToInt(bval);

                if ( val < 0 ) {
                    throw new IllegalStateException("data < 0");
                }
                                           
                val++;
                                                    
                zk.setData(counterPath, CassandraUtils.intToByteArray(val), -1);        
               
                return val;
              
            } catch (KeeperException e) {
              
                if(e instanceof KeeperException.NoNodeException){
                    
                    try {
                        zk.create(counterPath, CassandraUtils.intToByteArray(0), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        continue;
                    } catch (KeeperException e1) {
                       
                    } catch (InterruptedException e1) {
                       
                    }
                }
                    
                breaker.failure();
            } catch (InterruptedException e) {
                breaker.failure();
            } finally {
                unlock(lockName);
            }
        }

    }

    @Override
    public int getCurrentDocId(String indexName) {
        // TODO Auto-generated method stub
        return 0;
    }

    

}
