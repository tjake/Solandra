package lucandra;

import java.util.concurrent.CountDownLatch;

public class CassandraDaemon extends org.apache.cassandra.thrift.CassandraDaemon
{
    
    private CountDownLatch startedLatch = new CountDownLatch(1);
    
    public void start()
    {
        
        startedLatch.countDown();
        
        // TODO Auto-generated method stub
        super.start();
    }

    public CountDownLatch getStartedLatch()
    {
        return startedLatch;
    }
    
}
