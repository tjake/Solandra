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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/**
 * 
 * @author jake
 * 
 */
public class LucandraTermEnum extends TermEnum {

    private final IndexReader indexReader;
    private final String indexName;

    private int termPosition;
    private Term[] termBuffer;
    private SortedMap<Term, Collection<IColumn>> termDocFreqBuffer;
    private SortedMap<Term, Collection<IColumn>> termCache;
//    private DocumentRank[]  sortedTermDocs; 
    
    
    // number of sequential terms to read initially
    private final int maxInitSize = 2;
    private final int maxChunkSize = 1024;
    private int actualInitSize = -1;
    private Term initTerm = null;
    private Term chunkBoundryTerm;
    private String currentField = null;
    private int chunkCount = 0;

    private final Term finalTerm = new Term(CassandraUtils.delimeter, CassandraUtils.finalToken);

    private static final Logger logger = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
 //       this.sortedTermDocs = new DocumentRank[indexReader.numDocs()];
 //       for(int i=0; i<sortedTermDocs.length; i++){
 //           sortedTermDocs[i] = new DocumentRank();
 //       }
        
        this.termPosition = 0;
    }

    public boolean skipTo(Term term) throws IOException {

        if (term == null)
            return false;

        loadTerms(term);
        
        currentField = term.field();

        return termBuffer.length == 0 ? false : true;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public int docFreq() {
        return termDocFreqBuffer.size();
    }

    @Override
    public boolean next() throws IOException {    
        
        if(termBuffer == null){
            skipTo(new Term(""));
        }
        
        termPosition++;
        
        boolean hasNext = termPosition < termBuffer.length;

        if (hasNext && termBuffer[termPosition].equals(finalTerm)) {
            termPosition++;
            hasNext = termPosition < termBuffer.length;
        }

        if (!hasNext) {

            // if we've already done init try grabbing more
            if ((chunkCount == 1 && actualInitSize == maxInitSize) || (chunkCount > 1 && actualInitSize == maxChunkSize)) {
                loadTerms(chunkBoundryTerm);
                hasNext = termBuffer.length > 0;
            } else if ((chunkCount == 1 && actualInitSize < maxInitSize) || (chunkCount > 1 && actualInitSize < maxChunkSize)) {
                hasNext = false;            
                
                loadTerms(initTerm); //start over at top (for facets)   
            }
            
            termPosition = 0;
        }

        return hasNext;
    }

    @Override
    public Term term() {
        return termBuffer[termPosition];
    }

    private void loadTerms(Term skipTo) {

        if(initTerm == null)
            initTerm = skipTo;
        
        // chose starting term
        byte[] startTerm = CassandraUtils.hashKeyBytes(
                    indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(skipTo)
                );
                
        // ending term. the initial query we don't care since
        // we only pull 2 terms, also we don't
        byte[] endTerm = new byte[]{};
      
        //The boundary condition for this search. currently the field.
        byte[] boundryTerm = CassandraUtils.hashKeyBytes(
                indexName + CassandraUtils.delimeter + 
                CassandraUtils.createColumnName(skipTo.field(), CassandraUtils.finalToken)
                );
        
        
        if ((!skipTo.equals(chunkBoundryTerm) || termPosition == 0) && termCache != null) {
            termDocFreqBuffer = termCache.subMap(skipTo, termCache.lastKey());
        } else {          
            termDocFreqBuffer = null;
        }

        if (termDocFreqBuffer != null) {

            termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
            termPosition = 0;

            logger.debug("Found " + startTerm + " in cache");
            return;
        } else if (chunkCount > 1 && actualInitSize < maxChunkSize) {
            
            //include last term
            if(skipTo.equals(chunkBoundryTerm) && termCache.containsKey(skipTo)){
                termBuffer = new Term[] {skipTo};
                termDocFreqBuffer = termCache.subMap(skipTo, termCache.lastKey());
            }else{
                termBuffer = new Term[] {};
            }
            
            termPosition = 0;
            return; // done!
        }

        chunkCount++;

        // The first time we grab just a few keys
        int count = maxInitSize;

        // otherwise we grab all the rest of the keys
        if (chunkBoundryTerm != null) {
            count = maxChunkSize;
            startTerm = CassandraUtils.hashKeyBytes(
                        indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(chunkBoundryTerm)
                    );
            
            
            //After first pass use the boundary term, since we know on pass 2 we are using the OPP
            endTerm = boundryTerm;
            
        }

        long start = System.currentTimeMillis();

        termDocFreqBuffer = new TreeMap<Term, Collection<IColumn>>();

        ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily);        
        SlicePredicate slicePredicate = new SlicePredicate();
       

        // Get all columns
        SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, true, Integer.MAX_VALUE);
        slicePredicate.setSlice_range(sliceRange);
        
        List<Row> rows;
        try {
            
            IPartitioner        p = StorageService.getPartitioner();
            
            AbstractBounds bounds = new Bounds(p.getToken(startTerm), p.getToken(endTerm));

            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(CassandraUtils.keySpace, columnParent, slicePredicate, bounds, 1), ConsistencyLevel.ONE);

            //rows = StorageProxy.readProtocol(Arrays.asList((ReadCommand)new SliceFromReadCommand(CassandraUtils.keySpace, startTerm, columnParent, new byte[] {}, new byte[]{},
            //               false, Integer.MAX_VALUE)), ConsistencyLevel.ONE);
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        // term to start with next time
        actualInitSize = rows.size();
        logger.info("Found " + rows.size() + " keys in range:" + startTerm + " to " + endTerm + " in " + (System.currentTimeMillis() - start) + "ms");

        if (actualInitSize > 0) {
            for (Row row : rows) {
   
                String key;
                try {
                    key = new String(row.key.key,"UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("This JVM doesn't support UTF-8");
                }
                
                // term keys look like wikipedia/body/wiki
                String termStr = key.substring(key.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());
                Term term = CassandraUtils.parseTerm(termStr);                 
                
                Collection<IColumn> columns = row.cf.getSortedColumns();
                
                logger.debug(termStr + " has " + columns.size());
                
                //check for tombstone keys or incorrect keys (from RP)
                if(columns.size() > 0 && term.field().equals(skipTo.field()) &&
                        //from this index
                        key.equals(CassandraUtils.hashKey(indexName+CassandraUtils.delimeter+term.field()+CassandraUtils.delimeter+term.text()))){
                    
                    if(!columns.iterator().next().isMarkedForDelete()){
                                                
                        termDocFreqBuffer.put(term, columns);
                    }
                }
            }

            if(!termDocFreqBuffer.isEmpty()){
                chunkBoundryTerm = termDocFreqBuffer.lastKey();
            }
        }

        // add a final key (excluded in submap below)
        termDocFreqBuffer.put(finalTerm, null);

        // put in cache
        for (Term termKey : termDocFreqBuffer.keySet()) {

            if (termCache == null) {
                termCache = termDocFreqBuffer;
            } else {
                termCache.putAll(termDocFreqBuffer);
            }

            indexReader.addTermEnumCache(termKey, this);
        }

        // cache the initial term too
        indexReader.addTermEnumCache(skipTo, this);

        termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});

        termPosition = 0;

        long end = System.currentTimeMillis();

        logger.info("loadTerms: " + startTerm + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }

    void loadFilteredTerms(Term term, List<String> docNums)  {
        long start = System.currentTimeMillis();
        ColumnParent parent = new ColumnParent();
        parent.setColumn_family(CassandraUtils.termVecColumnFamily);

        byte[] key = CassandraUtils.hashKeyBytes(
                indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(term)
            );


        List<byte[]> columns = new ArrayList<byte[]>();
        for (String docNum : docNums) {
            columns.add(docNum.getBytes());
        }
      

        List<Row> rows = null;
        
        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, key, parent, columns);

        int attempts = 0;
        while (attempts++ < 10) {
            try {
                rows = StorageProxy.readProtocol(Arrays.asList(rc), ConsistencyLevel.ONE);
                break;
            } catch (IOException e1) {
               throw new RuntimeException(e1);
            } catch (UnavailableException e1) {
                
            } catch (TimeoutException e1) {
               
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                
            }
        }

        if(attempts >= 10)
            throw new RuntimeException("Read command failed after 10 attempts");
        
        termBuffer = new Term[0];

        if (rows != null  && rows.size()>0 && rows.get(0) != null && rows.get(0).cf != null && rows.get(0).cf.getSortedColumns() != null && rows.get(0).cf.getSortedColumns().size() > 0){
            termBuffer = new Term[1];
            termBuffer[0] = term;
            termDocFreqBuffer = new TreeMap<Term, Collection<IColumn>>();
            termDocFreqBuffer.put(term, rows.get(0).cf.getSortedColumns());
        }
        long end = System.currentTimeMillis();
        logger.debug("loadFilterdTerms: " + term + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }
    
    public final DocumentRank[] getTermDocFreq() {
        if (termBuffer.length == 0)
            return null;

        Collection<IColumn> termDocs = termDocFreqBuffer.get(termBuffer[termPosition]);

        long start = System.currentTimeMillis();
        
        // create proper docIds.
        // Make sure these ids are sorted in ascending order since lucene
        // requires this.
        DocumentRank docIds[] = new DocumentRank[termDocs.size()];
        int idx = 0;

        for (IColumn col : termDocs) {
            int docId = indexReader.addDocument(col, currentField);
            
            docIds[idx++] = new DocumentRank(docId,col);
        }

        long end = System.currentTimeMillis();
        
        //logger.info("termDocFreq: phase 1 in "+(end-start)+"ms");
        
        // sort
        Arrays.sort(docIds);

        end = System.currentTimeMillis();
        //logger.info("termDocFreq: phase 2 in "+(end-start)+"ms");

        
       return docIds;
    }

    public Set<Term> getCachedTerms() {
        return termCache.keySet();
    }

}
