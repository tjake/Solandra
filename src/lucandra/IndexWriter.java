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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TopDocs;
import org.apache.thrift.TException;

public class IndexWriter {

    private final String indexName;
    private final Cassandra.Iface client;
    private final ColumnPath docAllColumnPath;
    private boolean autoCommit;
    private Map<String,Map<String,List<Mutation>>> mutationMap;
    
    private Similarity similarity = Similarity.getDefault(); // how to normalize;     
    
    private static final Logger logger = Logger.getLogger(IndexWriter.class);

    public IndexWriter(String indexName, Cassandra.Iface client) {

        this.indexName = indexName;
        this.client = client;
        autoCommit  = true;
        docAllColumnPath = new ColumnPath(CassandraUtils.docColumnFamily);
        
        mutationMap = new HashMap<String,Map<String,List<Mutation>>>();
    }

    @SuppressWarnings("unchecked")
    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        List<String> allIndexedTerms = new ArrayList<String>();
        
        
        //check for special field name
        String docId = doc.get(CassandraUtils.documentIdField);
 
        if(docId == null)
            docId = Long.toHexString((long) (System.nanoTime()+(Math.random()*System.nanoTime()))); 
        
        int position = 0;
     
        for (Fieldable field : (List<Fieldable>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term information per field
                Map<String, Map<String,List<Number>>> allTermInformation = new HashMap<String, Map<String,List<Number>>>();
                
                int lastOffset = 0;
                if (position > 0) {
                    position += analyzer.getPositionIncrementGap(field.name());
                }

                // Build the termPositions vector for all terms
	          
                tokens.reset(); // reset the TokenStream to the first token
                
                // set up token attributes we are working on
                
                //offsets
                OffsetAttribute             offsetAttribute  = null;
                if(field.isStoreOffsetWithTermVector())
                    offsetAttribute = (OffsetAttribute) tokens.addAttribute(OffsetAttribute.class);
                
                //positions
                PositionIncrementAttribute  posIncrAttribute = null;
                if(field.isStorePositionWithTermVector())
                    posIncrAttribute = (PositionIncrementAttribute) tokens.addAttribute(PositionIncrementAttribute.class);
                
                TermAttribute               termAttribute    = (TermAttribute) tokens.addAttribute(TermAttribute.class);

                //store normalizations of field per term per document rather than per field.
                //this adds more to write but less to read on other side
                Integer tokensInField = new Integer(0);
                
                while (tokens.incrementToken()  ) {
                    tokensInField++;
                    String term = CassandraUtils.createColumnName(field.name(),termAttribute.term());
                	
                	allIndexedTerms.add(term);

                	//fetch all collected information for this term
                	Map<String,List<Number>> termInfo = allTermInformation.get(term);

                	if (termInfo == null) {
                		termInfo = new HashMap<String,List<Number>>();
                		allTermInformation.put(term, termInfo);
                	}

                	//term frequency
                	{
                	   List<Number> termFrequency = termInfo.get(CassandraUtils.termFrequencyKey);
                	               	
                	   if(termFrequency == null){
                	       termFrequency = new ArrayList<Number>();
                	       termFrequency.add(new Integer(0));
                	       termInfo.put(CassandraUtils.termFrequencyKey, termFrequency);
                	   }
                	
                	   //increment
                	   termFrequency.set(0, termFrequency.get(0).intValue()+1);                	   
                	}
                	
                	               	
                	//position vector
                	if(field.isStorePositionWithTermVector()){
                	    position += (posIncrAttribute.getPositionIncrement() - 1);
                	    
                	    List<Number> positionVector = termInfo.get(CassandraUtils.positionVectorKey);
                	    
                	    if(positionVector == null){
                	        positionVector = new ArrayList<Number>();
                	        termInfo.put(CassandraUtils.positionVectorKey, positionVector);
                	    }
                	    
                        positionVector.add(++position);
                	}
                	
                	//term offsets
                	if(field.isStoreOffsetWithTermVector()){

                	    List<Number> offsetVector = termInfo.get(CassandraUtils.offsetVectorKey);
                	    if(offsetVector == null){
                	        offsetVector = new ArrayList<Number>();
                	        termInfo.put(CassandraUtils.offsetVectorKey, offsetVector);
                	    }
                	    
                	    offsetVector.add( lastOffset + offsetAttribute.startOffset());
                        offsetVector.add( lastOffset + offsetAttribute.endOffset());
                        
                	}              	                	
                }

                List<Number> bnorm = null;
                if(!field.getOmitNorms()){
                    bnorm = new ArrayList<Number>();
                    float norm = doc.getBoost();
                    norm *= field.getBoost();
                    norm *= similarity.lengthNorm(field.name(), tokensInField);
                    bnorm.add(Similarity.encodeNorm(norm));
                }
                
                for (Map.Entry<String, Map<String,List<Number>>> term : allTermInformation.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all columns
                    // in a key/column family into memory
                    String key = indexName + CassandraUtils.delimeter + term.getKey();
                    
                    //Mix in the norm for this field alongside each term
                    //more writes but faster on read side.
                    if(!field.getOmitNorms()){
                        term.getValue().put(CassandraUtils.normsKey, bnorm );
                    }
                    
                    CassandraUtils.addToMutationMap(mutationMap, CassandraUtils.termVecColumnFamily, docId.getBytes("UTF-8"), CassandraUtils.hashKey(key), null,term.getValue());                    
                }
            }

            //Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized()) {
                String term = CassandraUtils.createColumnName(field.name(), field.stringValue());
                allIndexedTerms.add(term);
                
                String key = indexName + CassandraUtils.delimeter + term;

                Map<String,List<Number>> termMap = new HashMap<String,List<Number>>();
                termMap.put(CassandraUtils.termFrequencyKey, CassandraUtils.emptyArray);
                termMap.put(CassandraUtils.positionVectorKey, CassandraUtils.emptyArray);
                
                CassandraUtils.addToMutationMap(mutationMap, CassandraUtils.termVecColumnFamily, docId.getBytes("UTF-8"), CassandraUtils.hashKey(key), null,termMap);
               
            }

            // Stores each field as a column under this doc key
            if (field.isStored()) {
                
                byte[] _value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes("UTF-8");         
                
                //first byte flags if binary or not
                byte[] value = new byte[_value.length+1];
                System.arraycopy(_value, 0, value, 0, _value.length);
                
                value[value.length-1] = (byte) (field.isBinary() ? Byte.MAX_VALUE : Byte.MIN_VALUE);
                
                String key = indexName+CassandraUtils.delimeter+docId;
                
                CassandraUtils.addToMutationMap(mutationMap, CassandraUtils.docColumnFamily, field.name().getBytes("UTF-8"), CassandraUtils.hashKey(key), value, null);
                            
            }
        }
        
        //Finally, Store meta-data so we can delete this document
        String key = indexName+CassandraUtils.delimeter+docId;
        
        CassandraUtils.addToMutationMap(mutationMap, CassandraUtils.docColumnFamily, CassandraUtils.documentMetaField.getBytes("UTF-8"), CassandraUtils.hashKey(key), CassandraUtils.toBytes(allIndexedTerms), null);
        
       
        
        if(autoCommit)
            CassandraUtils.robustBatchInsert(client, mutationMap);    
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        
        IndexReader   reader   = new IndexReader(indexName,client);
        IndexSearcher searcher = new IndexSearcher(reader);
       
        TopDocs results = searcher.search(query,1000);
    
        for(int i=0; i<results.totalHits; i++){
            ScoreDoc doc = results.scoreDocs[i];
            
            
            String docId = reader.getDocumentId(doc.doc);
            try {
                deleteLucandraDocument(docId.getBytes("UTF-8"));
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (NotFoundException e) {
                throw new RuntimeException(e);
            } catch (UnavailableException e) {
                throw new RuntimeException(e);
            } catch (TimedOutException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);       
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
        try {
                       
            ColumnParent cp = new ColumnParent(CassandraUtils.termVecColumnFamily);
            String key = indexName+CassandraUtils.delimeter+CassandraUtils.createColumnName(term);
            
            List<ColumnOrSuperColumn> docs = client.get_slice(CassandraUtils.keySpace, CassandraUtils.hashKey(key), cp, new SlicePredicate().setSlice_range(new SliceRange(new byte[]{}, new byte[]{},true,Integer.MAX_VALUE)), ConsistencyLevel.ONE);
                
            //delete by documentId
            for(ColumnOrSuperColumn docInfo : docs){
                deleteLucandraDocument(docInfo.getSuper_column().getName());
            }
                              
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (TimedOutException e) {
            throw new RuntimeException(e);
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }  
    }
    
    private void deleteLucandraDocument(byte[] docId) throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException, IOException, ClassNotFoundException{

        String key =  indexName+CassandraUtils.delimeter+new String(docId);
        
        ColumnOrSuperColumn column = client.get(CassandraUtils.keySpace, CassandraUtils.hashKey(key), CassandraUtils.metaColumnPath, ConsistencyLevel.ONE);
        
        List<String> terms = (List<String>) CassandraUtils.fromBytes(column.column.value);
    
        for(String termStr : terms){
            
            key = indexName+CassandraUtils.delimeter+termStr;
            
            CassandraUtils.addToMutationMap(mutationMap, CassandraUtils.termVecColumnFamily, docId, CassandraUtils.hashKey(key), null, null);                                        
        }
    
        
        if(autoCommit)
            CassandraUtils.robustBatchInsert(client, mutationMap);
        
        //finally delete ourselves
        String selfKey = indexName+CassandraUtils.delimeter+new String(docId);
        
        
        //FIXME: once cassandra batch mutation supports slice predicates in deletions
        client.remove(CassandraUtils.keySpace, CassandraUtils.hashKey(selfKey), docAllColumnPath, System.currentTimeMillis(), ConsistencyLevel.ONE);

        
    }
    
    
    public void updateDocument(Term updateTerm, Document doc, Analyzer analyzer) throws CorruptIndexException, IOException{   
        
        deleteDocuments(updateTerm);
        addDocument(doc, analyzer);
        
    }

    public int docCount() {

        try{
            String start = CassandraUtils.hashKey(indexName + CassandraUtils.delimeter);
            String finish = start+CassandraUtils.delimeter;

            ColumnParent columnParent = new ColumnParent(CassandraUtils.docColumnFamily);
            SlicePredicate slicePredicate = new SlicePredicate();

            // Get all columns
            SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, true, Integer.MAX_VALUE);
            slicePredicate.setSlice_range(sliceRange);

            List<KeySlice> columns  = client.get_range_slice(CassandraUtils.keySpace, columnParent, slicePredicate, start, finish, 5000, ConsistencyLevel.ONE);
        
            return columns.size();
            
        }catch(Exception e){
            throw new RuntimeException(e);
        }
       
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }
    
    public void commit(){
        if(!autoCommit)
            CassandraUtils.robustBatchInsert(client, mutationMap);
    }

}
