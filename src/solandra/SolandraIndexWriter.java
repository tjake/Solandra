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
package solandra;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lucandra.CassandraUtils;
import lucandra.cluster.CassandraIndexManager;
import lucandra.cluster.IndexManagerService;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;

public class SolandraIndexWriter extends UpdateHandler
{

    private final lucandra.IndexWriter writer;
    private final static Logger        logger                          = Logger.getLogger(SolandraIndexWriter.class);

    // stats
    AtomicLong                         addCommands                     = new AtomicLong();
    AtomicLong                         addCommandsCumulative           = new AtomicLong();
    AtomicLong                         deleteByIdCommands              = new AtomicLong();
    AtomicLong                         deleteByIdCommandsCumulative    = new AtomicLong();
    AtomicLong                         deleteByQueryCommands           = new AtomicLong();
    AtomicLong                         deleteByQueryCommandsCumulative = new AtomicLong();
    AtomicLong                         expungeDeleteCommands           = new AtomicLong();
    AtomicLong                         mergeIndexesCommands            = new AtomicLong();
    AtomicLong                         commitCommands                  = new AtomicLong();
    AtomicLong                         optimizeCommands                = new AtomicLong();
    AtomicLong                         rollbackCommands                = new AtomicLong();
    AtomicLong                         numDocsPending                  = new AtomicLong();
    AtomicLong                         numErrors                       = new AtomicLong();
    AtomicLong                         numErrorsCumulative             = new AtomicLong();

    public SolandraIndexWriter(SolrCore core)
    {
        super(core);

        try
        {

            writer = new lucandra.IndexWriter();

        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public int addDoc(AddUpdateCommand cmd) throws IOException {

        addCommands.incrementAndGet();
        addCommandsCumulative.incrementAndGet();
        int rc = -1;

        // no duplicates allowed            
        SchemaField uniqueField = core.getSchema().getUniqueKeyField();
        
        if(uniqueField == null)
            throw new IOException("Solandra requires a unique field");
        
        // if there is no ID field, use allowDups
        if (idField == null) {
            throw new IOException("Solandra requires a unique field");   
        }

       try{
           
           String indexName = core.getName();
           String key       = cmd.getIndexedId(schema);
           
            Long docId =  IndexManagerService.instance.getId(indexName, key);
                
            boolean isUpdate = false;
            if(docId != null)
            {
                isUpdate = true;
            } 
            else
            {
                docId = IndexManagerService.instance.getNextId(indexName, key);
            }
            
            int shard     = CassandraIndexManager.getShardFromDocId(docId);
            int shardedId = CassandraIndexManager.getShardedDocId(docId);
            indexName = core.getName()+"~"+shard;
            
            if(logger.isDebugEnabled())
                logger.debug("Adding "+shardedId+" to "+indexName);
            
            writer.setIndexName(indexName);
                       
            Term idTerm = this.idTerm.createTerm(cmd.indexedId);
                       
            if(isUpdate)
                writer.updateDocument(idTerm, cmd.getLuceneDocument(schema), schema.getAnalyzer(), shardedId);                
            else
                writer.addDocument(cmd.getLuceneDocument(schema), schema.getAnalyzer(), shardedId);

            rc = 1;
            
       }finally {
            if (rc != 1) {
                numErrors.incrementAndGet();
                numErrorsCumulative.incrementAndGet();
            }
        }

        return rc;

    }

    public void close() throws IOException
    {
        // hehe
    }

    public void commit(CommitUpdateCommand cmd) throws IOException
    {
        // hehe
    }

    public void delete(DeleteUpdateCommand cmd) throws IOException
    {
        deleteByIdCommands.incrementAndGet();
        deleteByIdCommandsCumulative.incrementAndGet();

        if (!cmd.fromPending && !cmd.fromCommitted)
        {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "meaningless command: " + cmd);
        }
        if (!cmd.fromPending || !cmd.fromCommitted)
        {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "operation not supported" + cmd);
        }

  
        Term term = idTerm.createTerm(idFieldType.toInternal(cmd.id));

        logger.info("Deleting term: "+term);
        
        ByteBuffer keyKey = CassandraUtils.hashKeyBytes((core.getName()+"~"+term.text()).getBytes(), CassandraUtils.delimeterBytes, "keys".getBytes());
        ByteBuffer keyCol= ByteBuffer.wrap(term.text().getBytes());
    
        List<Row> rows = CassandraUtils.robustRead(keyKey, new QueryPath(CassandraUtils.schemaInfoColumnFamily), Arrays.asList(keyCol), ConsistencyLevel.QUORUM);

        if(rows.size() == 1)
        {
            Row row = rows.get(0);
            
            if(row.cf != null)
            {
               IColumn col = row.cf.getColumn(keyCol);
               
               if(col != null){
                   ByteBuffer idCol = col.getSubColumns().iterator().next().name();
                   Long  id  = Long.valueOf(ByteBufferUtil.string(idCol));                   
                   int shard = CassandraIndexManager.getShardFromDocId(id);
                   int sid   = CassandraIndexManager.getShardedDocId(id);
                   
                   ByteBuffer sidName = ByteBuffer.wrap(String.valueOf(sid).getBytes());
                   
                   String subIndex = core.getName() + "~" + shard;
                  
                   //Delete all terms/fields/etc
                   writer.setIndexName(subIndex);
                   writer.deleteDocuments(term);
                   
                   //Delete key -> docId lookup
                   RowMutation rm = new RowMutation(CassandraUtils.keySpace, keyKey);
                   rm.delete(new QueryPath(CassandraUtils.schemaInfoColumnFamily, keyCol), System.currentTimeMillis()-10);
                   
                   //Delete docId so it can be reused
                   //TODO: update shard info with docid
                   ByteBuffer idKey = CassandraUtils.hashKeyBytes(subIndex.getBytes(), CassandraUtils.delimeterBytes, "ids".getBytes());
                   RowMutation rm2 = new RowMutation(CassandraUtils.keySpace, idKey);
                   rm2.delete(new QueryPath(CassandraUtils.schemaInfoColumnFamily, sidName), System.currentTimeMillis()-10);
                   
                   CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm, rm2);
               }
            }        
        }
    }

    public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException
    {
        deleteByQueryCommands.incrementAndGet();
        deleteByQueryCommandsCumulative.incrementAndGet();

        if (!cmd.fromPending && !cmd.fromCommitted)
        {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "meaningless command: " + cmd);
        }
        if (!cmd.fromPending || !cmd.fromCommitted)
        {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "operation not supported" + cmd);
        }

        boolean madeIt = false;
        boolean delAll = false;
        try
        {
            Query q = QueryParsing.parseQuery(cmd.query, schema);
            delAll = MatchAllDocsQuery.class == q.getClass();

            if (delAll)
            {

                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "can't delete all: " + cmd);
            }
            else
            {
                writer.deleteDocuments(q);
            }

            madeIt = true;

        }
        finally
        {
            if (!madeIt)
            {
                numErrors.incrementAndGet();
                numErrorsCumulative.incrementAndGet();
            }
        }

    }

    public int mergeIndexes(MergeIndexesCommand cmd) throws IOException
    {
        // haha
        return 0;
    }

    public void rollback(RollbackUpdateCommand cmd) throws IOException
    {
        // TODO Auto-generated method stub

    }

    public Category getCategory()
    {
        return Category.UPDATEHANDLER;
    }

    public String getDescription()
    {
        return "Update handler for Solandra";
    }

    public URL[] getDocs()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String getName()
    {
        return SolandraIndexWriter.class.getName();
    }

    public String getSource()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public String getSourceId()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @SuppressWarnings("unchecked")
    public NamedList getStatistics()
    {
        NamedList lst = new SimpleOrderedMap();

        lst.add("rollbacks", rollbackCommands.get());
        lst.add("adds", addCommands.get());
        lst.add("deletesById", deleteByIdCommands.get());
        lst.add("deletesByQuery", deleteByQueryCommands.get());
        lst.add("errors", numErrors.get());
        lst.add("cumulative_adds", addCommandsCumulative.get());
        lst.add("cumulative_deletesById", deleteByIdCommandsCumulative.get());
        lst.add("cumulative_deletesByQuery", deleteByQueryCommandsCumulative.get());
        lst.add("cumulative_errors", numErrorsCumulative.get());
        return lst;
    }

    public String getVersion()
    {
        return core.getVersion();
    }

}
