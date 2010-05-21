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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.thrift.TException;

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
    private SortedMap<Term, List<ColumnOrSuperColumn>> termDocFreqBuffer;
    private SortedMap<Term, List<ColumnOrSuperColumn>> termCache;

    // number of sequential terms to read initially
    private final int maxInitSize = 2;
    private final int maxChunkSize = 1024;
    private int actualInitSize = -1;
    private Term initTerm = null;
    private Term chunkBoundryTerm;
    private String currentField = null;
    private int chunkCount = 0;

    private final Cassandra.Iface client;
    private final Term finalTerm = new Term(CassandraUtils.delimeter, CassandraUtils.finalToken);

    private static final Logger logger = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
        this.client = indexReader.getClient();
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
        String startTerm = CassandraUtils.hashKey(
                    indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(skipTo)
                );
                
        // ending term. the initial query we don't care since
        // we only pull 2 terms, also we don't
        String endTerm = "";
      
        //The boundary condition for this search. currently the field.
        String boundryTerm = CassandraUtils.hashKey(
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
            startTerm = CassandraUtils.hashKey(
                        indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(chunkBoundryTerm)
                    );
            
            
            //After first pass use the boundary term, since we know on pass 2 we are using the OPP
            endTerm = boundryTerm;
            
        }

        long start = System.currentTimeMillis();

        termDocFreqBuffer = new TreeMap<Term, List<ColumnOrSuperColumn>>();

        ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily);        
        SlicePredicate slicePredicate = new SlicePredicate();
       

        // Get all columns
        SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, true, Integer.MAX_VALUE);
        slicePredicate.setSlice_range(sliceRange);
        
        List<KeySlice> columns;
        try {
            columns = client.get_range_slice(CassandraUtils.keySpace, columnParent, slicePredicate, startTerm, endTerm, count, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TimedOutException e) {
            throw new RuntimeException(e);
        }

        // term to start with next time
        actualInitSize = columns.size();
        logger.debug("Found " + columns.size() + " keys in range:" + startTerm + " to " + endTerm + " in " + (System.currentTimeMillis() - start) + "ms");

        if (actualInitSize > 0) {
            for (KeySlice entry : columns) {
   
                // term keys look like wikipedia/body/wiki
                String termStr = entry.getKey().substring(entry.getKey().indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());
                Term term = CassandraUtils.parseTerm(termStr);                 
                
                logger.debug(termStr + " has " + entry.getColumns().size());
                
                //check for tombstone keys or incorrect keys (from RP)
                if(entry.getColumns().size() > 0 && term.field().equals(skipTo.field()) &&
                        //from this index
                        entry.getKey().equals(CassandraUtils.hashKey(indexName+CassandraUtils.delimeter+term.field()+CassandraUtils.delimeter+term.text())))
                    
                    termDocFreqBuffer.put(term, entry.getColumns());
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

        logger.debug("loadTerms: " + startTerm + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }

    void loadFilteredTerms(Term term, List<String> docNums)  {
        long start = System.currentTimeMillis();
        ColumnParent parent = new ColumnParent();
        parent.setColumn_family(CassandraUtils.termVecColumnFamily);

        String key = CassandraUtils.hashKey(
                indexName + CassandraUtils.delimeter + CassandraUtils.createColumnName(term)
            );

        SlicePredicate slicePredicate = new SlicePredicate();

        
        for (String docNum : docNums) {
            slicePredicate.addToColumn_names(docNum.getBytes());
        }

        

        List<ColumnOrSuperColumn> columsList = null;
        try {
            columsList = client.get_slice(CassandraUtils.keySpace, key, parent, slicePredicate, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TimedOutException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }

        termBuffer = new Term[0];

        if (columsList != null  && columsList.size()>0){
            termBuffer = new Term[1];
            termBuffer[0] = term;
            termDocFreqBuffer = new TreeMap<Term, List<ColumnOrSuperColumn>>();
            termDocFreqBuffer.put(term, columsList);
        }
        long end = System.currentTimeMillis();
        logger.debug("loadFilterdTerms: " + term + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }
    
    public final List<ColumnOrSuperColumn> getTermDocFreq() {
        if (termBuffer.length == 0)
            return null;

        List<ColumnOrSuperColumn> termDocs = termDocFreqBuffer.get(termBuffer[termPosition]);

        // create proper docIds.
        // Make sure these ids are sorted in ascending order since lucene
        // requires this.
        int docIds[] = new int[termDocs.size()];
        int idx = 0;
        List<ColumnOrSuperColumn> sortedTermDocs = new ArrayList<ColumnOrSuperColumn>(termDocs.size());
        Map<Integer, ColumnOrSuperColumn> termDocMap = new HashMap<Integer, ColumnOrSuperColumn>();

        for (ColumnOrSuperColumn col : termDocs) {
            int docId = indexReader.addDocument(col.getSuper_column(), currentField);
            termDocMap.put(docId, col);
            docIds[idx++] = docId;
        }

        // sort
        Arrays.sort(docIds);

        // move
        for (idx = 0; idx < termDocs.size(); idx++) {
            sortedTermDocs.add(termDocMap.get(docIds[idx]));
        }

        return sortedTermDocs;
    }

    public Set<Term> getCachedTerms() {
        return termCache.keySet();
    }

}
