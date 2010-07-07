package lucandra;

import org.apache.cassandra.db.IColumn;

public class DocumentRank implements Comparable<DocumentRank> {
    public int rank;
    public IColumn column;
    
    public DocumentRank(){
        
    }
    
    public DocumentRank(int rank, IColumn column){
        this.rank   = rank;
        this.column = column;
    }

    @Override
    public int compareTo(DocumentRank o) {
        if(this.rank > o.rank )
            return 1;
        if(this.rank < o.rank)
            return -1;
        
        return 0;
    }
}
