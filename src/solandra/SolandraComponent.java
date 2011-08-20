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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import lucandra.CassandraUtils;
import lucandra.IndexReader;
import lucandra.cluster.CassandraIndexManager;
import lucandra.cluster.IndexManagerService;

import com.google.common.collect.MapMaker;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.SolrIndexReader;

public final class SolandraComponent 
{
    private static AtomicBoolean hasSolandraSchema = new AtomicBoolean(false);
    private static final Logger logger = Logger.getLogger(SolandraComponent.class);
    public final static Map<String,Long> cacheCheck = new MapMaker().makeMap();

    public static boolean flushCache(String indexName) throws IOException
    {   
        if(CassandraUtils.cacheInvalidationInterval == 0)
            return true;
        
        Long lastCheck = SolandraComponent.cacheCheck.get(indexName);
    
        if(lastCheck == null || lastCheck <= (System.currentTimeMillis() - CassandraUtils.cacheInvalidationInterval))
        {
        
            ByteBuffer keyKey = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes, "cache".getBytes("UTF-8"));

            List<Row> rows = CassandraUtils.robustRead(keyKey, new QueryPath(CassandraUtils.schemaInfoColumnFamily, CassandraUtils.cachedColBytes), Arrays
                    .asList(CassandraUtils.cachedColBytes), ConsistencyLevel.QUORUM);
            
            
            SolandraComponent.cacheCheck.put(indexName, System.currentTimeMillis());
            
            
            if(lastCheck == null || rows == null || rows.isEmpty() || rows.get(0).cf == null ||             
                    rows.get(0).cf.getColumn(CassandraUtils.cachedColBytes).getSubColumn(CassandraUtils.cachedColBytes).timestamp() >= lastCheck)
            {
                if(logger.isDebugEnabled())
                    logger.debug("Flushed cache: "+indexName);                 
                    
                return true;
            }
        }
              
        return false;
    }
    
    public static boolean prepare(ResponseBuilder rb) throws IOException
    {

        // Only applies to my lucandra index readers
        if (!(((SolrIndexReader) rb.req.getSearcher().getIndexReader()).getWrappedReader() instanceof lucandra.IndexReader))
            return false;
        
        if(!hasSolandraSchema.get())
        {
            //Check is Solandra schema exists, if not die
            if(! DatabaseDescriptor.getNonSystemTables().contains(CassandraUtils.keySpace) )
                throw new IOException("Solandra keyspace is missing, please import then retry");
            else
                hasSolandraSchema.set(true);
        }
            
        // If this is a shard request then no need to do anything
        if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false))
        {
            String indexName = (String) rb.req.getContext().get("solandra-index");

            if (indexName == null)
                throw new IOException("Missing core name");

            IndexReader reader = (IndexReader) ((SolrIndexReader) rb.req.getSearcher().getIndexReader()).getWrappedReader();

            reader.setIndexName(indexName);
            
            if(flushCache(indexName))
                reader.reopen();
            
            return false;
        }

        String indexName = (String) rb.req.getContext().get("solandra-index");

        if (indexName == null || indexName.equals(""))
        {
            return false; // 
        }
        else
        {
            if(logger.isDebugEnabled())
                logger.debug("core: " + indexName);
        }

        if (rb.shards == null)
        {
            // find number of shards
            long docId = IndexManagerService.instance.getMaxId(indexName);

            int numShards = CassandraIndexManager.getShardFromDocId(docId);

            //run local
            if(numShards == 0) {
                IndexReader reader = (IndexReader) ((SolrIndexReader) rb.req.getSearcher().getIndexReader())
                .getWrappedReader();

                String subIndex = indexName+"~0";
                reader.setIndexName(subIndex);
                
                if(flushCache(subIndex))
                {
                    reader.reopen();                
                }
                return false;
            }
            
            // assign shards
            String[] shards = new String[numShards + 1];

            for (int i = 0; i <= numShards; i++)
            {
                ByteBuffer subIndex = CassandraUtils.hashBytes((indexName + "~" + i).getBytes("UTF-8"));
                
                List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(CassandraUtils.keySpace, subIndex);
                DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

                if (endpoints.isEmpty())
                    throw new IOException("can't locate index");

                InetAddress addr = endpoints.get(0);
                String shard = addr.getHostAddress() + ":" + CassandraUtils.port + "/solandra/" + indexName + "~" + i;

                if(logger.isDebugEnabled())
                    logger.debug("Adding shard(" + indexName + "): " + shard);

                shards[i] = shard;
            }

            // assign to shards
            rb.shards = shards;
            
            return true;
        }
        
        return false;
    }
}
