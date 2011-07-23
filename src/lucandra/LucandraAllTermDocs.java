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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import lucandra.cluster.CassandraIndexManager;

import org.apache.cassandra.db.*;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.OpenBitSet;

public class LucandraAllTermDocs implements TermDocs
{

    private static Logger logger    = Logger.getLogger(LucandraAllTermDocs.class);
    private String        indexName;
    private int           idx;      // tracks where we are in the doc buffer
    private int           fillSize; // tracks how much the buffer was filled with docs from cassandra
    private OpenBitSet    docBuffer = new OpenBitSet(CassandraIndexManager.maxDocsPerShard+1); // max number of docs we pull
    private int           doc       = -1;
    private int           maxDoc;

    public LucandraAllTermDocs(IndexReader indexReader)
    {
        indexName = indexReader.getIndexName();
        maxDoc = indexReader.maxDoc();
        
        idx = 0;
        fillSize = 0;
        
        
        try
        {
            fillDocBuffer();
        }
        catch (IOException e)
        {
            logger.error(e);
            throw new RuntimeException(e);
        }

    }

    public void seek(Term term) throws IOException
    {
        if (term == null)
        {
            doc = -1;
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public void seek(TermEnum termEnum) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int doc()
    {
        return doc;
    }

    public int freq()
    {
        return 1;
    }

    public boolean next() throws IOException
    {
        return skipTo(doc + 1);
    }

    public int read(int[] docs, int[] freqs) throws IOException
    {
        final int length = docs.length;
        int i = 0;
        while (i < length && doc < maxDoc && fillSize > 0)
        {

            docs[i]  = doc;
            freqs[i] = docBuffer.getBit(doc);
            ++i;

            next();
        }
        return i;
    }

    public boolean skipTo(int target) throws IOException
    {
        doc = target;
        for (; idx < maxDoc; idx++)
        {
                if (idx >= doc && docBuffer.getBit(idx) > 0){
                    doc = idx;
                    return true; 
                }
        }

        return false;
    }

    public void close() throws IOException
    {
    }

    private void fillDocBuffer() throws IOException
    {
        
        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes, "ids".getBytes("UTF-8"));

        ByteBuffer startCol = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        int pageSize = 4096;
        
        while(true)
        {
        
            ReadCommand cmd = new SliceFromReadCommand(CassandraUtils.keySpace, key,
                    new ColumnParent(CassandraUtils.schemaInfoColumnFamily), startCol,
                    ByteBufferUtil.EMPTY_BYTE_BUFFER, false, pageSize);

        
            List<Row> rows = CassandraUtils.robustRead(CassandraUtils.consistency, cmd);

            if(rows.isEmpty())
                return;
        
            Row row = rows.get(0);

            if(row == null || row.cf == null)
                return;
        
            int actualPageSize = 0;
            for(IColumn sc : row.cf.getSortedColumns()){
                        
                actualPageSize++;
                              
                if(sc.name().equals(startCol))
                    continue;
                
                startCol = sc.name();

                if(!sc.isLive())
                    continue;
                                                           
                Integer id  = Integer.valueOf(ByteBufferUtil.string(sc.name()));

                docBuffer.set(id);
                fillSize++;               
            }     
            
            if(actualPageSize < pageSize)
                break;
            
            if(logger.isDebugEnabled())
                logger.debug("Read "+fillSize+ " "+ ByteBufferUtil.string(startCol));
        }
    }
}
