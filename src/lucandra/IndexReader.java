/**
 * Copyright 2009 T Jake Luciani
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.store.Directory;

public class IndexReader extends org.apache.lucene.index.IndexReader {

    private final static int numDocs = 1000000;
    private final static byte[] norms = new byte[numDocs];
    static{
        Arrays.fill(norms, DefaultSimilarity.encodeNorm(1.0f));
    }
    
    private final String indexName;
    private final Cassandra.Iface client;
    private final Map<String,Integer> docIdToDocIndex;
    private final Map<Integer,String> docIndexToDocId;
    private final AtomicInteger docCounter;
   
    private final Map<Term, LucandraTermEnum> termEnumCache;
   

    private static final Logger logger = Logger.getLogger(IndexReader.class);

    public IndexReader(String name, Cassandra.Iface client) {
        super();
        this.indexName = name;
        this.client = client;
        

        docCounter         = new AtomicInteger(0);
        docIdToDocIndex    = new HashMap<String,Integer>();
        docIndexToDocId    = new HashMap<Integer,String>();
        
        termEnumCache = new HashMap<Term, LucandraTermEnum>();
    }
    
  
    @Override
    public IndexReader reopen(){
        
        docCounter.set(0);
        docIdToDocIndex.clear();
        docIndexToDocId.clear();
        termEnumCache.clear();
        
        return this;
    }
    
    @Override
    protected void doClose() throws IOException {
        
    } 
    
    @Override
    protected void doCommit() throws IOException {
      
    }

    @Override
    protected void doDelete(int arg0) throws CorruptIndexException, IOException {
       // throw new UnsupportedOperationException();
    }

    @Override
    protected void doSetNorm(int arg0, String arg1, byte arg2) throws CorruptIndexException, IOException {
       // throw new UnsupportedOperationException();
    }

    @Override
    protected void doUndeleteAll() throws CorruptIndexException, IOException {
        //throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq(Term term) throws IOException {

        LucandraTermEnum termEnum = termEnumCache.get(term);
        if (termEnum == null) {

            long start = System.currentTimeMillis();

            termEnum = new LucandraTermEnum(this);
            termEnum.skipTo(term);

            long end = System.currentTimeMillis();

            logger.info("docFreq() took: " + (end - start) + "ms");

            termEnumCache.put(term, termEnum);   
        }
        
        return termEnum.docFreq();
    }

    @Override
    public Document document(int docNum, FieldSelector selector) throws CorruptIndexException, IOException {

        String key = indexName +CassandraUtils.delimeter+docIndexToDocId.get(docNum);
        
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumnFamily);
        

        //get all columns
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(new SliceRange(new byte[] {}, CassandraUtils.delimeter.getBytes(), false, 100));

        long start = System.currentTimeMillis();

        try {
            List<ColumnOrSuperColumn> cols = client.get_slice(CassandraUtils.keySpace, CassandraUtils.hashKey(key), columnParent, slicePredicate, ConsistencyLevel.ONE);

            Document doc = new Document();
            for (ColumnOrSuperColumn col : cols) {
                
                Field  field = null;
                String fieldName = new String(col.column.name); 
                
                byte[] value;
                
                if(col.column.value[col.column.value.length-1] != Byte.MAX_VALUE && col.column.value[col.column.value.length-1] != Byte.MIN_VALUE){
                    value = col.column.value; //support backwards compatibility
                    field = new Field(fieldName, new String(value), Store.YES, Index.ANALYZED);
                    
                }else if( col.column.value[col.column.value.length-1] == Byte.MAX_VALUE){
                    value = new byte[col.column.value.length-1];
                    System.arraycopy(col.column.value, 0, value, 0, col.column.value.length-1);
                    
                    field = new Field(fieldName, value, Store.YES);
                }else if( col.column.value[col.column.value.length-1] == Byte.MIN_VALUE){
                    value = new byte[col.column.value.length-1];
                    System.arraycopy(col.column.value, 0, value, 0, col.column.value.length-1);
                    
                    field = new Field(fieldName, new String(value), Store.YES, Index.ANALYZED);
                }
                
        
                doc.add(field);
            }

            
            
            long end = System.currentTimeMillis();

            logger.debug("Document read took: " + (end - start) + "ms");

            return doc;

        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }

    }

    @Override
    public Collection getFieldNames(FieldOption fieldOption) {     
       return Arrays.asList(new String[]{});
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
       
        LucandraTermEnum termEnum = termEnumCache.get(term);
        
        if(termEnum == null){
        
            termEnum = new LucandraTermEnum(this);
            if( !termEnum.skipTo(term) )           
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
        }

     
        return idx;
    }

    public String getDocumentId(int docNum) {
        return docIndexToDocId.get(docNum);
    }

    public String getIndexName() {
        return indexName;
    }

    public Cassandra.Iface getClient() {
        return client;
    }
    
    public LucandraTermEnum checkTermCache(Term term){
        return termEnumCache.get(term);
    }
    
    public void addTermEnumCache(Term term, LucandraTermEnum termEnum){
        termEnumCache.put(term, termEnum);
    }
    
    @Override
    public Directory directory() {
            return null;
    }


    @Override
    public long getVersion() {
            return 1;
    }

}
