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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

public class LucandraAllTermDocs implements TermDocs
{

    private static Logger logger    = Logger.getLogger(LucandraAllTermDocs.class);
    private String        indexName;
    private int           idx;      // tracks where we are in the doc buffer
    private int           fillSize; // tracks how much the buffer was filled with docs from cassandra
    private int[]         docBuffer = new int[CassandraUtils.maxDocsPerShard+1]; // max number of docs we pull
    private int           doc       = -1;
    private int           maxDoc;

    public LucandraAllTermDocs(IndexReader indexReader)
    {
        indexName = indexReader.getIndexName();
        maxDoc = indexReader.maxDoc();
        Arrays.fill(docBuffer, 0);
        
        idx = 0;
        fillSize = 0;
        
        fillDocBuffer();

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

            docs[i] = doc;
            freqs[i] = docBuffer[doc];
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
                if (idx >= doc && docBuffer[idx] > 0){
                    doc = idx;
                    return true; 
                }
        }

        return false;
    }

    public void close() throws IOException
    {
    }

    private void fillDocBuffer()
    {
        
        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, "ids".getBytes());

        ReadCommand cmd = new SliceFromReadCommand(CassandraUtils.keySpace, key,
                new ColumnParent(CassandraUtils.schemaInfoColumnFamily), FBUtilities.EMPTY_BYTE_BUFFER,
                FBUtilities.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);

        
        List<Row> rows = CassandraUtils.robustRead(CassandraUtils.consistency, cmd);

        if(rows.isEmpty())
            return;
        
        Row row = rows.get(0);

        if(row == null || row.cf == null)
            return;
        
        for(IColumn sc : row.cf.getSortedColumns()){
            Integer id   = Integer.valueOf(ByteBufferUtil.string(sc.name()));
            
            for(IColumn c : sc.getSubColumns())
            {
                //valid id
                if( !(c instanceof ExpiringColumn)){
                    docBuffer[id] = 1;
                    fillSize++;
                }
            }
        }     
    }

}
