package lucandra;

import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class IndexReader extends org.apache.lucene.index.IndexReader {

    private final String keySpace = "Lucandra";
    private final String indexName; 
    private final Cassandra.Client client;
    
    public IndexReader(String name, Cassandra.Client client) throws TTransportException {
       super();
       this.indexName = name;
       this.client    = client;
    }
    
    @Override
    protected void doClose() throws IOException {
        //nothing
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
        columnParent.setSuper_column("Terms".getBytes());
        columnParent.setColumn_family(term.field()+"|x|"+term.text());
        
        try {
            return client.get_count(keySpace, indexName, columnParent, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Document document(int docNum, FieldSelector selector) throws CorruptIndexException, IOException {
        
        byte[] docId = CassandraUtils.intToByteArray(docNum);
        client.get_slice(CassandraUtils.keySpace, indexName, column_parent, predicate, consistency_level)
        
    }
    
    @Override
    public Collection getFieldNames(FieldOption arg0) {
        // TODO Auto-generated method stub
        return null;
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
    public byte[] norms(String arg0) throws IOException {
        return null;
    }

    @Override
    public void norms(String arg0, byte[] arg1, int arg2) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int numDocs() {
        ColumnParent columnParent = new ColumnParent();
        columnParent.setSuper_column("Documents".getBytes());

        try {
            return client.get_count(keySpace, indexName, columnParent, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TermDocs termDocs() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TermPositions termPositions() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TermEnum terms() throws IOException {
        return new LucandraTermEnum(indexName, client);
    }

    @Override
    public TermEnum terms(Term term) throws IOException {
       TermEnum termEnum = new LucandraTermEnum(indexName,client);
       
       return termEnum.skipTo(term) ? termEnum : null;
    }

}
