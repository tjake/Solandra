package lucandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.thrift.TException;

public class IndexReader extends org.apache.lucene.index.IndexReader {

    private final String indexName; 
    private final Cassandra.Client client;
    private final Set<Long>   documentSet;
    private final List<Long>  documents;
   
    
    public IndexReader(String name, Cassandra.Client client) {
       super();
       this.indexName = name;
       this.client    = client;
       
       documents   = new ArrayList<Long>();
       documentSet = new HashSet<Long>();
    }
    
    @Override
    protected void doClose() throws IOException {
        documents.clear();
        documentSet.clear();
    }

    @Override
    protected void doCommit() throws IOException {
        //nothing
    }

    @Override
    protected void doDelete(int arg0) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doSetNorm(int arg0, String arg1, byte arg2) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doUndeleteAll() throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq(Term term) throws IOException {
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.termVecColumn);
        columnParent.setSuper_column(CassandraUtils.createColumnName(term).getBytes());
        
        try {
            return client.get_count(CassandraUtils.keySpace, indexName, columnParent, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Document document(int docNum, FieldSelector selector) throws CorruptIndexException, IOException {
        
        byte[] docId = CassandraUtils.encodeLong(documents.get(docNum));    
        
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumn);
        columnParent.setSuper_column(docId);
        
        SlicePredicate slicePredicate = new SlicePredicate();
       
        slicePredicate.setSlice_range(new SliceRange(new byte[]{},new byte[]{},false,100));
        
        try{
            List<ColumnOrSuperColumn> cols = client.get_slice(CassandraUtils.keySpace, indexName, columnParent, slicePredicate, ConsistencyLevel.ONE);
        
            Document doc = new Document();
            for(ColumnOrSuperColumn col : cols){
                Field field = new Field(new String(col.column.name,"UTF-8"), col.column.value,Store.YES);
                
                doc.add(field);
            }
            
            return doc;
            
        }catch(Exception e){
            throw new IOException(e);
        }
        
    }
    
    @Override
    public Collection getFieldNames(FieldOption arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TermFreqVector getTermFreqVector(int arg0, String arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void getTermFreqVector(int arg0, TermVectorMapper arg1) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void getTermFreqVector(int arg0, String arg1, TermVectorMapper arg2) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public TermFreqVector[] getTermFreqVectors(int arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasDeletions() {
        
        return false;
    }

    @Override
    public boolean isDeleted(int arg0) {
        
        return false;
    }

    @Override
    public int maxDoc() {
        return numDocs()+1;
    }

    @Override
    public byte[] norms(String term) throws IOException {        
        byte[] ones = new byte[this.maxDoc()];
        Arrays.fill(ones, DefaultSimilarity.encodeNorm(1.0f));
        return ones;
    }

    @Override
    public void norms(String arg0, byte[] arg1, int arg2) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int numDocs() {
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumn);

        try {
            return client.get_count(CassandraUtils.keySpace, indexName, columnParent, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TermDocs termDocs() throws IOException {
        return new LucandraTermDocs(this);
    }

    @Override
    public TermPositions termPositions() throws IOException {
        return new LucandraTermDocs(this);
    }

    @Override
    public TermEnum terms() throws IOException {
        return new LucandraTermEnum(this);
    }

    @Override
    public TermEnum terms(Term term) throws IOException {
       TermEnum termEnum = new LucandraTermEnum(this);
       
       return termEnum.skipTo(term) ? termEnum : null;
    }

    public int addDocument(byte[] docId){
        
        long id = CassandraUtils.decodeLong(docId);
             
        if(!documents.contains(id)){
            documents.add(id);
            documentSet.add(id);
            
            return documents.size()-1;
            
        }
        
        return documents.indexOf(id);       
    }
    
    public long getDocumentId(int docNum){
        return documents.get(docNum);
    }
    
   
    
    public String getIndexName() {
        return indexName;
    }

    public Cassandra.Client getClient() {
        return client;
    }

}
