package lucandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.NotFoundException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.SuperColumn;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.thrift.TException;

public class LucandraTermEnum extends TermEnum {

    private final String           indexName;
    private int                    termPosition;       
    private List<Term>             termBuffer;
    private List<Integer>          termDocFreqBuffer;
    private final int              bufferSize; 
    private final Cassandra.Client    client;
    
    public LucandraTermEnum(String indexName, Cassandra.Client client){
        this.indexName  = indexName;
        this.client     = client;
        this.bufferSize = 100;
        this.termPosition = 0;
        this.termBuffer        = new ArrayList<Term>(bufferSize);
        this.termDocFreqBuffer = new ArrayList<Integer>(bufferSize);
    }
    
    @Override
    public boolean skipTo(Term term){
        loadTerms(term);
        
        return termBuffer.isEmpty() ? false : true;
            
    }
     
    @Override
    public void close() throws IOException {
        
    }

    @Override
    public int docFreq() {
        return termDocFreqBuffer.get(termPosition);
    }

    @Override
    public boolean next() throws IOException {
        
        if(termPosition >= termBuffer.size() && termBuffer.size()<bufferSize){
            loadTerms(null);
        }else{
            termPosition++;
        }
        
        if(termPosition < termBuffer.size()){
            return true;
        }else{
            return false;
        }
        
    }

    @Override
    public Term term() {
        return termBuffer.get(termPosition);
    }

    private void loadTerms(Term skipTo){
        
      
        ColumnParent columnParent = new ColumnParent();
        columnParent.setSuper_column(CassandraUtils.termColumn);
      
        
        //create predicate
        SlicePredicate slicePredicate = new SlicePredicate();
        SliceRange     sliceRange     = new SliceRange();
        slicePredicate.setSlice_range(sliceRange);
        
        //chose starting term
        String startTerm = "";
        
        if(skipTo != null ){
        
            startTerm = CassandraUtils.createColumnName(skipTo);
    
        } else if(!termBuffer.isEmpty()){
            Term endTerm = termBuffer.get(termBuffer.size()-1);
            if(endTerm != null){
                startTerm = CassandraUtils.createColumnName(endTerm);
            }
        }
        
        sliceRange.setStart(startTerm.getBytes());
        sliceRange.setCount(bufferSize);
        
        List<ColumnOrSuperColumn> termColumns;
        
        try {
            termColumns = client.get_slice(CassandraUtils.keySpace, indexName,columnParent, slicePredicate, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        
        termBuffer.clear();
        termDocFreqBuffer.clear(); 
        
        //parse results
        for(ColumnOrSuperColumn termColumn : termColumns){
            SuperColumn termSuperColumn = termColumn.getSuper_column();
            
            termBuffer.add(CassandraUtils.parseTerm(termSuperColumn.getName()));
            termDocFreqBuffer.add(termSuperColumn.getColumns().size());
        }
        
        termPosition = 0;
    }
    
}
