package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.UnavailableException;
import org.apache.log4j.Logger;
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

import com.sun.org.apache.xerces.internal.dom.DocumentImpl;

public class IndexReader extends org.apache.lucene.index.IndexReader {

    private final static int numDocs = 1000000;
    private final static byte[] norms = new byte[numDocs];
    static{
        Arrays.fill(norms, DefaultSimilarity.encodeNorm(1.0f));
    }
    
    private final String indexName;
    private final Cassandra.Client client;
    private final Map<String,Integer> docIdToDocIndex;
    private final Map<Integer,String> docIndexToDocId;
    private final AtomicInteger docCounter;
   
    private final Map<Term, LucandraTermEnum> termCache;


    private static final Logger logger = Logger.getLogger(IndexReader.class);

    public IndexReader(String name, Cassandra.Client client) {
        super();
        this.indexName = name;
        this.client = client;

        docCounter         = new AtomicInteger(0);
        docIdToDocIndex    = new HashMap<String,Integer>();
        docIndexToDocId    = new HashMap<Integer,String>();
        
        termCache = new HashMap<Term, LucandraTermEnum>();
    }

    @Override
    protected void doClose() throws IOException {
        docCounter.set(0);
        docIdToDocIndex.clear();
        docIndexToDocId.clear();
        termCache.clear();
    }

    @Override
    protected void doCommit() throws IOException {
        // nothing
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

        LucandraTermEnum termEnum = termCache.get(term);
        if (termEnum == null) {

            long start = System.currentTimeMillis();

            termEnum = new LucandraTermEnum(this);
            termEnum.skipTo(term);

            long end = System.currentTimeMillis();

            //logger.info("docFreq() took: " + (end - start) + "ms");

            termCache.put(term, termEnum);   
        }
        
        return termEnum.docFreq();
    }

    @Override
    public Document document(int docNum, FieldSelector selector) throws CorruptIndexException, IOException {

        //byte[] docId = CassandraUtils.encodeLong(docIndexToDocId.get(docNum));

        String key = indexName +"/"+docIndexToDocId.get(docNum);
        
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumnFamily);
        

        //get all columns
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(new SliceRange(new byte[] {}, new byte[] {}, false, 100));

        long start = System.currentTimeMillis();

        try {
            List<ColumnOrSuperColumn> cols = client.get_slice(CassandraUtils.keySpace, key, columnParent, slicePredicate, ConsistencyLevel.ONE);

            Document doc = new Document();
            for (ColumnOrSuperColumn col : cols) {
                Field field = new Field(new String(col.column.name, "UTF-8"), col.column.value, Store.YES);

                doc.add(field);
            }

            long end = System.currentTimeMillis();

            logger.info("Document read took: " + (end - start) + "ms");

            return doc;

        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
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
        //if (numDocs == null)
        //    numDocs();

        return numDocs + 1;
    }

    @Override
    public byte[] norms(String term) throws IOException {
        return norms;     
    }

    @Override
    public void norms(String arg0, byte[] arg1, int arg2) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int numDocs() {

        return numDocs;
        
        //get count is too slow!!!
        /*if (numDocs != null)
            return numDocs;

        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumn);

        try {
            long start = System.currentTimeMillis();
            int num = client.get_count(CassandraUtils.keySpace, indexName, columnParent, ConsistencyLevel.ONE);
            long end = System.currentTimeMillis();

            logger.info("numDocs took: " + (end - start) + "ms");

            numDocs = num;

            return numDocs;
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } */
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
       
        LucandraTermEnum termEnum = termCache.get(term);
        
        if(termEnum == null){
        
            termEnum = new LucandraTermEnum(this);
            if( termEnum.skipTo(term) ) 
                termCache.put(term, termEnum);
            else
                termEnum = null;
            
        }
        
        return termEnum;
    }

    public int addDocument(byte[] docId) {

        
        String id;
        try {
            id = new String(docId,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Cant make docId a string");
        }
        
        Integer idx = docIdToDocIndex.get(id);
        
        if(idx == null){
            idx = docCounter.incrementAndGet();

            if(idx > numDocs)
                throw new IllegalStateException("numDocs reached");
            
            docIdToDocIndex.put(id, idx);
            docIndexToDocId.put(idx, id);

            return idx;
        }

        return idx;
    }

    public String getDocumentId(int docNum) {
        return docIndexToDocId.get(docNum);
    }

    public String getIndexName() {
        return indexName;
    }

    public Cassandra.Client getClient() {
        return client;
    }
    
    public LucandraTermEnum checkTermCache(Term term){
        return termCache.get(term);
    }

}
