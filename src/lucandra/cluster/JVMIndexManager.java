package lucandra.cluster;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class JVMIndexManager extends AbstractIndexManager {

    private final AtomicLong counter      = new AtomicLong(-1);
    
    public JVMIndexManager(int shardsAtOnce){
        super(shardsAtOnce);
    }

    @Override
    public long internalFetch(String indexName) {
       return counter.get();
    }

    @Override
    public long internalIncrement(String indexName) {
        return counter.incrementAndGet();
    }

    @Override
    public void resetCounter(String indexName) {
        counter.set(-1);
    }
    
}
