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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.UnavailableException;
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
    private Map<Term, SortedMap<Term, List<ColumnOrSuperColumn>>> termCache;

    //number of sequential terms to read initially
    private final int maxInitSize = 16;
    private int actualInitSize = -1;
    private Term initTerm;
    
    
    private final Cassandra.Client client;

    private static final Logger logger = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
        this.client = indexReader.getClient();
        this.termPosition = 0;

        this.termCache = new HashMap<Term, SortedMap<Term, List<ColumnOrSuperColumn>>>();
    }

    @Override
    public boolean skipTo(Term term) throws IOException {
        loadTerms(term);

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

        termPosition++;

        boolean hasNext = termPosition < termBuffer.length;

        if (!hasNext) {           
        
            //if we've already done init try grabbing more
            if(actualInitSize == maxInitSize){
                loadTerms(initTerm);
                hasNext = termBuffer.length > 0;
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
                  
        // chose starting term
        String startTerm = indexName + "/" + CassandraUtils.createColumnName(skipTo);
        // this is where we stop;
        String endTerm = startTerm + new Character((char) 255);

        if(!skipTo.equals(initTerm) || termPosition == 0) {
            termDocFreqBuffer = termCache.get(skipTo);
        }else{
            termDocFreqBuffer = null;
        }
            
        if (termDocFreqBuffer != null) {

            termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
            termPosition = 0;

            logger.debug("Found " + startTerm + " in cache");
            return;
        }

        //The first time we grab just a few keys
        int count = maxInitSize;
        
        //otherwise we grab all the rest of the keys 
        if( initTerm != null ){
            count = Integer.MAX_VALUE; 
        }
            
        long start = System.currentTimeMillis();

        // First buffer the keys in this term range
        List<String> keys;
        try {
            keys = client.get_key_range(CassandraUtils.keySpace, CassandraUtils.termVecColumnFamily, startTerm, endTerm, count,
                    ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }

        logger.debug("Found " + keys.size() + " keys in range:" + startTerm + " to " + endTerm + " in " + (System.currentTimeMillis() - start)+"ms");

        if(initTerm == null){
            initTerm = skipTo;
            actualInitSize = keys.size();
        }else{
            keys.subList(0,actualInitSize).clear();
            actualInitSize = -1; 
            initTerm = null;
        }
            
        termDocFreqBuffer = new TreeMap<Term, List<ColumnOrSuperColumn>>();

        if (!keys.isEmpty()) {
            ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily, null);
            SlicePredicate slicePredicate = new SlicePredicate();

            // Get all columns
            SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, false, Integer.MAX_VALUE);
            slicePredicate.setSlice_range(sliceRange);

            Map<String, List<ColumnOrSuperColumn>> columns;

            try {
                columns = client.multiget_slice(CassandraUtils.keySpace, keys, columnParent, slicePredicate, ConsistencyLevel.ONE);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (UnavailableException e) {
                throw new RuntimeException(e);
            }

            for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : columns.entrySet()) {

                // term keys look like wikipedia/body|x|wiki
                String termStr = entry.getKey().substring(entry.getKey().indexOf("/")+1);
                Term term = CassandraUtils.parseTerm(termStr.getBytes());
                
                termDocFreqBuffer.put(term, entry.getValue());
            }
        }

        //add a final key (excluded in submap below)
        termDocFreqBuffer.put(new Term(""+new Character((char)255), ""+new Character((char)255)), null);
        
        // put in cache
        for (Term termKey : termDocFreqBuffer.keySet()) {
                      
            SortedMap<Term, List<ColumnOrSuperColumn>> subMap = termDocFreqBuffer.subMap(termKey, termDocFreqBuffer.lastKey());
            
            logger.debug("Caching "+termKey+" with "+subMap.size()+" siblings");
            termCache.put(termKey, subMap);
            
            indexReader.addTermEnumCache(termKey, this);
        }

      
        termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
        termPosition = 0;

        long end = System.currentTimeMillis();

        logger.debug("loadTerms: " + startTerm + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }

    public final List<ColumnOrSuperColumn> getTermDocFreq() {
        if (termBuffer.length == 0)
            return null;

        List<ColumnOrSuperColumn> termDocs = termDocFreqBuffer.get(termBuffer[termPosition]);

        return termDocs;
    }
    
    public Set<Term> getCachedTerms(){
        return termCache.keySet();
    }

}
