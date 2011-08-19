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
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lucandra.CassandraUtils;
import lucandra.Pair;
import lucandra.cluster.CassandraIndexManager;
import lucandra.cluster.IndexManagerService;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolandraCoreContainer;
import org.apache.solr.core.SolandraCoreInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;

import com.google.common.collect.MapMaker;

public class SolandraIndexWriter extends UpdateHandler
{
    // To manage cached reads
    private static final ConcurrentLinkedQueue<String>        flushQueue                      = new ConcurrentLinkedQueue<String>();
    private final ExecutorService                             flushMonitor                    = Executors
                                                                                                      .newSingleThreadExecutor();

    private final static MultiThreadedHttpConnectionManager   httpConnections                 = new MultiThreadedHttpConnectionManager();

    private final static lucandra.IndexWriter                 writer                          = new lucandra.IndexWriter();
    private final static Logger                               logger                          = Logger.getLogger(SolandraIndexWriter.class);

    // stats
    AtomicLong                                                addCommands                     = new AtomicLong();
    AtomicLong                                                addCommandsCumulative           = new AtomicLong();
    AtomicLong                                                deleteByIdCommands              = new AtomicLong();
    AtomicLong                                                deleteByIdCommandsCumulative    = new AtomicLong();
    AtomicLong                                                deleteByQueryCommands           = new AtomicLong();
    AtomicLong                                                deleteByQueryCommandsCumulative = new AtomicLong();
    AtomicLong                                                expungeDeleteCommands           = new AtomicLong();
    AtomicLong                                                mergeIndexesCommands            = new AtomicLong();
    AtomicLong                                                commitCommands                  = new AtomicLong();
    AtomicLong                                                optimizeCommands                = new AtomicLong();
    AtomicLong                                                rollbackCommands                = new AtomicLong();
    AtomicLong                                                numDocsPending                  = new AtomicLong();
    AtomicLong                                                numErrors                       = new AtomicLong();
    AtomicLong                                                numErrorsCumulative             = new AtomicLong();

    private static final ConcurrentMap<String, AtomicInteger> bufferedWrites                  = new MapMaker()
                                                                                                      .makeMap();
    private static final int                                  writeTreshold                   = Integer
                                                                                                      .valueOf(CassandraUtils.properties
                                                                                                              .getProperty(
                                                                                                                      "solandra.write.buffer.queue.size",
                                                                                                                      "16"));

    private static final int port = Integer.parseInt(System.getProperty("solandra.port", "8983"));

    public SolandraIndexWriter(SolrCore core)
    {
        super(core);

        try
        {

            flushMonitor.execute(new Runnable() {

                public void run()
                {
                    Map<String, Long> lastCoreFlush = new HashMap<String, Long>();
                    Long lastBufferFlush = new Long(0);

                    while (true)
                    {
                        String core = flushQueue.poll();

                        if (core != null)
                        {

                            Long lastFlush = lastCoreFlush.get(core);
                            if (lastFlush == null
                                    || lastFlush <= (System.currentTimeMillis() - CassandraUtils.cacheInvalidationInterval))
                            {
                                try
                                {
                                    flush(core);
                                }
                                catch (IOException e)
                                {
                                    throw new RuntimeException(e);
                                }
                                lastCoreFlush.put(core, System.currentTimeMillis());
                                if (logger.isDebugEnabled())
                                    logger.debug("Flushed cache: " + core);
                            }
                        }
                        else
                        {

                            if (lastBufferFlush < System.currentTimeMillis()
                                    - CassandraUtils.cacheInvalidationInterval)
                            {

                                // Flush any buffered writes
                                for (Map.Entry<String, AtomicInteger> entry : bufferedWrites.entrySet())
                                {
                                    if (entry.getValue().intValue() > 0)
                                    {
                                        writer.commit(entry.getKey(), false);
                                        entry.getValue().set(0);
                                    }
                                }
                                
                                lastBufferFlush = System.currentTimeMillis();
                            }
                            else
                            {
                                try
                                {
                                    Thread.sleep(20);
                                }
                                catch (InterruptedException e)
                                {
                                   
                                }
                            }
                            
                        }
                    }
                }
            });

        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void flush(String core) throws IOException
    {
        // Make sure all writes are in for this core
        writer.commit(core, true);

        ByteBuffer cacheKey = CassandraUtils.hashKeyBytes((core).getBytes("UTF-8"), CassandraUtils.delimeterBytes,
                "cache".getBytes("UTF-8"));

        RowMutation rm = new RowMutation(CassandraUtils.keySpace, cacheKey);
        rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, CassandraUtils.cachedColBytes,
                CassandraUtils.cachedColBytes), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);

        // also directly notify the local readers
        SolandraComponent.cacheCheck.put(core, System.currentTimeMillis() - CassandraUtils.cacheInvalidationInterval
                - 1);

    }

    public int addDoc(AddUpdateCommand cmd) throws IOException
    {

        addCommands.incrementAndGet();
        addCommandsCumulative.incrementAndGet();
        int rc = -1;

        // no duplicates allowed
        SchemaField uniqueField = core.getSchema().getUniqueKeyField();

        if (uniqueField == null)
            throw new IOException("Solandra requires a unique field");

        // if there is no ID field, use allowDups
        if (idField == null)
        {
            throw new IOException("Solandra requires a unique field");
        }

        try
        {

            SolandraCoreInfo coreInfo = SolandraCoreContainer.coreInfo.get();
            String key = cmd.getIndexedId(schema);

            Long docId = null;
            RowMutation[] rms = null;
            
            if (!coreInfo.bulk && !cmd.allowDups)
                docId = IndexManagerService.instance.getId(coreInfo.indexName, key);

            boolean isUpdate = false;
            if (docId != null)
            {
                isUpdate = true;
                            
                if(logger.isDebugEnabled())
                    logger.debug("update for document " + docId);
             
            }
            else
            {

                rms = new RowMutation[3];

                docId = IndexManagerService.instance.getNextId(coreInfo.indexName, key, rms);

                if (logger.isDebugEnabled())
                    logger.debug("new document " + docId);
            }

            int shard = CassandraIndexManager.getShardFromDocId(docId);
            int shardedId = CassandraIndexManager.getShardedDocId(docId);
            String indexName = coreInfo.indexName + "~" + shard;

            // logger.info("adding doc to"+indexName);

            if (logger.isDebugEnabled())
                logger.debug("Adding " + shardedId + " to " + indexName);

            Term idTerm = this.idTerm.createTerm(cmd.indexedId);

            if (isUpdate)
            {
                writer.updateDocument(indexName, idTerm, cmd.getLuceneDocument(schema), schema.getAnalyzer(),
                        shardedId, false);
            
                writer.appendMutations(indexName, IndexManagerService.instance.getIdMutation(coreInfo.indexName, key, docId));
            }
            else
            {
                writer.addDocument(indexName, cmd.getLuceneDocument(schema), schema.getAnalyzer(), shardedId, false,
                        rms);
            }
            rc = 1;

            // Notify readers
            tryCommit(indexName);

        }
        finally
        {
            if (rc != 1)
            {
                numErrors.incrementAndGet();
                numErrorsCumulative.incrementAndGet();
            }
        }

        return rc;

    }

    public void close() throws IOException
    {
    }

    public void commit(CommitUpdateCommand cmd) throws IOException
    {

        commitCommands.incrementAndGet();

        String indexName = SolandraCoreContainer.coreInfo.get().indexName;
        long maxId = IndexManagerService.instance.getMaxId(indexName);
        int maxShard = CassandraIndexManager.getShardFromDocId(maxId);

        for (int i = 0; i <= maxShard; i++)
        {
            if (logger.isDebugEnabled())
                logger.debug("committing " + indexName + "~" + i);
            commit(indexName + "~" + i, true);
        }
    }

    public void commit(String indexName, boolean blocked) throws IOException
    {
        writer.commit(indexName, blocked);

        if (blocked)
            flush(indexName);
        else
            SolandraIndexWriter.flushQueue.add(indexName);
    }

    public void delete(DeleteUpdateCommand cmd) throws IOException
    {
        String indexName = SolandraCoreContainer.coreInfo.get().indexName;

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

        if (logger.isDebugEnabled())
            logger.debug("Deleting term: " + term);

        ByteBuffer keyKey = CassandraUtils.hashKeyBytes((indexName + "~" + term.text()).getBytes("UTF-8"),
                CassandraUtils.delimeterBytes, "keys".getBytes("UTF-8"));
        ByteBuffer keyCol = ByteBuffer.wrap(term.text().getBytes("UTF-8"));

        List<Row> rows = CassandraUtils.robustRead(keyKey, new QueryPath(CassandraUtils.schemaInfoColumnFamily),
                Arrays.asList(keyCol), ConsistencyLevel.QUORUM);

        if (rows.size() == 1)
        {
            Row row = rows.get(0);

            if (row.cf != null)
            {
                IColumn col = row.cf.getColumn(keyCol);

                if (col != null)
                {
                    ByteBuffer idCol = col.getSubColumns().iterator().next().name();
                    Long id = Long.valueOf(ByteBufferUtil.string(idCol));
                    int shard = CassandraIndexManager.getShardFromDocId(id);
                    int sid = CassandraIndexManager.getShardedDocId(id);

                    ByteBuffer sidName = ByteBuffer.wrap(String.valueOf(sid).getBytes("UTF-8"));

                    String subIndex = indexName + "~" + shard;

                    // Delete all terms/fields/etc
                    writer.deleteDocuments(subIndex, term, false);

                    // Delete key -> docId lookup
                    RowMutation rm = new RowMutation(CassandraUtils.keySpace, keyKey);
                    rm.delete(new QueryPath(CassandraUtils.schemaInfoColumnFamily, keyCol), System.currentTimeMillis());

                    // Delete docId so it can be reused
                    // TODO: update shard info with this docid
                    ByteBuffer idKey = CassandraUtils.hashKeyBytes(subIndex.getBytes("UTF-8"),
                            CassandraUtils.delimeterBytes, "ids".getBytes("UTF-8"));
                    RowMutation rm2 = new RowMutation(CassandraUtils.keySpace, idKey);
                    rm2.delete(new QueryPath(CassandraUtils.schemaInfoColumnFamily, sidName),
                            System.currentTimeMillis());

                    CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm, rm2);

                    // Notify readers
                    tryCommit(subIndex);
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

        try
        {

            SolandraCoreInfo coreInfo = SolandraCoreContainer.coreInfo.get();
            String indexName = coreInfo.indexName;

            List<String> localShards = new ArrayList<String>();

            if (coreInfo.isShard)
            {
                localShards.add(coreInfo.indexName + "~" + coreInfo.shard);
            }
            else
            {
                long maxId = IndexManagerService.instance.getMaxId(indexName);
                Integer maxShard = CassandraIndexManager.getShardFromDocId(maxId);

                for (int i = 0; i <= maxShard; i++)
                {
                    String subIndex = indexName + "~" + i;

                    InetAddress addr = getIndexLocation(subIndex);
                    if (addr.equals(FBUtilities.getLocalAddress()))
                    {
                        // mark for local delete
                        localShards.add(subIndex);
                    }
                    else
                    {
                        // delete from other shards via http
                        CommonsHttpSolrServer solrj = new CommonsHttpSolrServer("http://" + addr.getHostAddress()
                                + ":" + port + "/solandra/" + subIndex, new HttpClient(httpConnections));

                        try
                        {
                            solrj.deleteByQuery(cmd.query);
                        }
                        catch (SolrServerException e)
                        {
                            throw new IOException(e);
                        }
                    }
                }
            }

            for (String subIndex : localShards)
            {
                Query q = QueryParsing.parseQuery(cmd.query, schema);
                long total = writer.deleteDocuments(subIndex, q, false);
                commit(subIndex, true);
                logger.info("Deleted " + total + " Documents in " + subIndex);
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

    private InetAddress getIndexLocation(String subIndex)
    {

        ByteBuffer indexName = CassandraUtils.hashBytes(subIndex.getBytes());

        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(CassandraUtils.keySpace,
                indexName);

        if (endpoints.isEmpty())
            throw new RuntimeException("Unable to find a live endpoint for: " + subIndex);

        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

        if (endpoints.contains(FBUtilities.getLocalAddress()))
            return FBUtilities.getLocalAddress();

        return endpoints.get(0);
    }

    public int mergeIndexes(MergeIndexesCommand cmd) throws IOException
    {
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

    private void tryCommit(String indexName) throws IOException
    {
        AtomicInteger times = bufferedWrites.get(indexName);

        if (times == null)
        {
            synchronized (indexName.intern())
            {
                times = bufferedWrites.get(indexName);
                if (times == null)
                {
                    times = new AtomicInteger(0);
                    bufferedWrites.putIfAbsent(indexName, times);
                }
            }
        }

        int t = times.incrementAndGet();

        if (t > writeTreshold)
        {
            commit(indexName, false);
            times.set(0);
        }
    }
}
