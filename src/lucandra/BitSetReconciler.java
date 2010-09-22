package lucandra;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.clock.AbstractReconciler;
import org.apache.log4j.Logger;

public class BitSetReconciler extends AbstractReconciler {

    private final static Logger logger = Logger.getLogger(BitSetReconciler.class);
    
    @Override
    public Column reconcile(Column left, Column right) {
       
        byte[] leftBytes  = left.value();
        byte[] rightBytes = right.value();
     
        
        
        //this puts both into leftBytes
        BitSetUtils.or(leftBytes, rightBytes);  
        return left;
    }
}
