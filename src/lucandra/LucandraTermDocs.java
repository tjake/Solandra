package lucandra;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.Column;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

public class LucandraTermDocs implements TermDocs {

    private IndexReader      indexReader;
    private LucandraTermEnum termEnum;
    private List<Column>     termDocs;
    private int              position;     
  
    
    public LucandraTermDocs(IndexReader indexReader){
        this.indexReader = indexReader;
        termEnum = new LucandraTermEnum(indexReader);
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int doc() {
       //track norms
       //track ids, etc...
       return indexReader.addDocument(termDocs.get(position).getName());
    }

    @Override
    public int freq() {
        return CassandraUtils.byteArrayToInt(termDocs.get(position).getValue());
    }

    @Override
    public boolean next() throws IOException {
        position++;
        
        return position < termDocs.size();
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
        
        int i=0;
        for(; (position<termDocs.size() && position<docs.length); i++,position++){
            docs[i]  = doc();
            freqs[i] = freq();
        }
        
        return i; 
    }

    @Override
    public void seek(Term term) throws IOException {
        //on a new term so flush the caches??
        
        
       termEnum.skipTo(term);
       termDocs = termEnum.getTermDocFreq();
       position = 0;
    }

    @Override
    public void seek(TermEnum termEnum) throws IOException {     
        if(termEnum instanceof LucandraTermEnum){
            this.termEnum = (LucandraTermEnum)termEnum;
            termDocs = this.termEnum.getTermDocFreq();
            position = 0;
        }else{
            throw new RuntimeException("TermEnum is not compatable with Lucandra");
        }
    }

    @Override
    public boolean skipTo(int arg0) throws IOException {
        throw new UnsupportedOperationException();
    }

}
