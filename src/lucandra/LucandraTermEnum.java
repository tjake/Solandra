/**
 * Copyright T Jake Luciani
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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.*;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

public class LucandraTermEnum extends TermEnum
{
    //Shared info for a given index
    private final IndexReader        indexReader;
    private final String             indexName;
    private final ReaderCache        readerCache;
    private final TermCache          termCache;
    
    //Local info this enum 
    private Map.Entry<Term, LucandraTermInfo[]>      currentTermEntry;
    private ConcurrentNavigableMap<Term, LucandraTermInfo[]> termView;
    
    
 
    private static final Logger                  logger         = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader)
    {
        this.indexReader = indexReader;
        indexName        = indexReader.getIndexName();
        readerCache      = indexReader.getCache();
        termCache        = readerCache.termCache; 
    }

    public boolean skipTo(Term term) throws IOException
    {
        if (term == null)
            return false;
        
        termView         = termCache.skipTo(term);            
        currentTermEntry = termView.firstEntry();
        
        return currentTermEntry != null;
    }
    
    @Override
    public void close() throws IOException
    {

    }

    @Override
    public int docFreq()
    {
        
        int freq = currentTermEntry == null ? 0 : currentTermEntry.getValue().length;
        return freq;
    }

    @Override
    public boolean next() throws IOException
    {
        //current term is in tree
        if(termView.size() < 2)
        {
            currentTermEntry = null;
            return false;
        }
        
        termView = termView.tailMap(currentTermEntry.getKey(), false);
        currentTermEntry = termView.firstEntry();
        
        //rebuffer on last key
        if(termView.size() == 1)
        {
           termView = termCache.skipTo(currentTermEntry.getKey());
        
           if(termView.size() < 2 && termView.firstEntry().getKey().equals(currentTermEntry))
           {
               currentTermEntry = null;
               return false;
           }
        }
        
        return true;      
    }

    @Override
    public Term term()
    {
        return currentTermEntry == null ? null : currentTermEntry.getKey();
    }

   
    public final LucandraTermInfo[] getTermDocFreq()
    {
        if(currentTermEntry == null)
            return null;

        Term term = currentTermEntry.getKey();

        LucandraTermInfo[] docIds = currentTermEntry.getValue();

        
        // set normalizations
        indexReader.addDocumentNormalizations(docIds, term.field(), readerCache);

        return docIds;
    }
    
    
    public LucandraTermInfo[] loadFilteredTerms(Term term, List<ByteBuffer> docNums) throws IOException
    {
        long start = System.currentTimeMillis();
        ColumnParent parent = new ColumnParent();
        parent.setColumn_family(CassandraUtils.termVecColumnFamily);
 
        ByteBuffer key;
        try
        {
            key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, term.field()
                    .getBytes(), CassandraUtils.delimeterBytes, term.text().getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException e2)
        {
            throw new RuntimeException("JVM doesn't support UTF-8", e2);
        }
 
        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, key, parent, docNums);
 
        List<Row> rows = CassandraUtils.robustRead(ConsistencyLevel.ONE, rc);
 
        LucandraTermInfo[] termInfo = null;
 
        if (rows != null && rows.size() > 0 && rows.get(0) != null && rows.get(0).cf != null)
        {     
            termInfo = TermCache.convertTermInfo(rows.get(0).cf.getSortedColumns());
        }
        
        long end = System.currentTimeMillis();
        
        if(logger.isDebugEnabled())
            logger.debug("loadFilterdTerms: " + term + "(" + termInfo == null ? 0 : termInfo.length + ") took " + (end - start) + "ms");
 
        return termInfo;
    }

}
