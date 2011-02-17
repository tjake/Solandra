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
package org.apache.solr.core;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.ParserConfigurationException;

import lucandra.CassandraUtils;
import lucandra.cluster.CassandraIndexManager;
import lucandra.cluster.IndexManagerService;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.SAXException;


public class SolandraCoreContainer extends CoreContainer
{
    public  final static ThreadLocal<SolandraCoreInfo>        coreInfo      = new ThreadLocal<SolandraCoreInfo>();
    public  final static ThreadLocal<HttpServletRequest>      activeRequest = new ThreadLocal<HttpServletRequest>();

    
    private static final Logger                              logger    = Logger.getLogger(SolandraCoreContainer.class);
    private final static InstrumentedCache<String, SolrCore> cache     = new InstrumentedCache<String, SolrCore>(1024);
    private final static QueryPath                           queryPath = new QueryPath(
                                                                               CassandraUtils.schemaInfoColumnFamily,
                                                                               CassandraUtils.schemaKeyBytes);

    private final String                                     solrConfigFile;
    private final SolrCore                                   singleCore;

    public SolandraCoreContainer(String solrConfigFile) throws ParserConfigurationException, IOException, SAXException
    {
        this.solrConfigFile = solrConfigFile;

        SolrConfig solrConfig = new SolrConfig(solrConfigFile);
        CoreDescriptor dcore = new CoreDescriptor(null, "", ".");

        singleCore = new SolrCore("", "/tmp", solrConfig, null, dcore);
    }

    @Override
    public SolrCore getCore(String name)
    {

        if (logger.isDebugEnabled())
            logger.info("Loading Solandra core: " + name);

        SolrCore core = null;

        if (name.equals(""))
        {
            core = singleCore;
        }
        else
        {

            SolandraCoreInfo indexInfo = new SolandraCoreInfo(name);

            coreInfo.set(indexInfo);
            
            try
            {
                core = readSchema(indexInfo.coreName);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            catch (ParserConfigurationException e)
            {
                throw new RuntimeException(e);
            }
            catch (SAXException e)
            {
                throw new RuntimeException(e);
            }
        }

        if (core != null)
        {
            synchronized (core)
            {
                core.open();
            }
        }

        return core;
    }
    
    public static String readSchemaXML(String indexName) throws IOException
    {        
        
        List<Row> rows = CassandraUtils.robustRead(ByteBuffer.wrap((indexName + "/schema").getBytes("UTF-8")), queryPath,
                Arrays.asList(CassandraUtils.schemaKeyBytes), ConsistencyLevel.QUORUM);

        if (rows.isEmpty())
            throw new IOException("invalid index: "+indexName);

        if (rows.size() > 1)
            throw new IllegalStateException("More than one schema found for this index");

        if (rows.get(0).cf == null)
            throw new IOException("invalid index: "+indexName);

        ByteBuffer schema = rows.get(0).cf.getColumn(CassandraUtils.schemaKeyBytes).getSubColumn(
                CassandraUtils.schemaKeyBytes).value();
        return ByteBufferUtil.string(schema);
    }

    public synchronized SolrCore readSchema(String indexName) throws IOException, ParserConfigurationException,
            SAXException
    {

        SolrCore core = cache.get(indexName);

        if (core == null)
        {
            // get from cassandra
            if (logger.isDebugEnabled())
                logger.debug("loading indexInfo for: " + indexName);

            List<Row> rows = CassandraUtils.robustRead(ByteBuffer.wrap((indexName + "/schema").getBytes("UTF-8")), queryPath,
                    Arrays.asList(CassandraUtils.schemaKeyBytes), ConsistencyLevel.QUORUM);

            if (rows.isEmpty())
                throw new IOException("invalid index");

            if (rows.size() > 1)
                throw new IllegalStateException("More than one schema found for this index");

            if (rows.get(0).cf == null)
                throw new IOException("invalid index");

            ByteBuffer buf = rows.get(0).cf.getColumn(CassandraUtils.schemaKeyBytes).getSubColumn(
                    CassandraUtils.schemaKeyBytes).value();
            InputStream stream = new ByteArrayInputStream(ByteBufferUtil.getArray(buf));

            SolrConfig solrConfig = new SolrConfig(solrConfigFile);

            IndexSchema schema = new IndexSchema(solrConfig, indexName, stream);

            core = new SolrCore(indexName, "/tmp", solrConfig, schema, null);

            logger.debug("Loaded core from cassandra: " + indexName);

            cache.put(indexName, core);
        }

        return core;
    }

    public static void writeSchema(String indexName, String schemaXml) throws IOException
    {
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, ByteBuffer.wrap((indexName + "/schema").getBytes("UTF-8")));

        try
        {

            rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily, CassandraUtils.schemaKeyBytes,
                    CassandraUtils.schemaKeyBytes), ByteBuffer.wrap(schemaXml.getBytes("UTF-8")), System
                    .currentTimeMillis());

        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);

        logger.debug("Wrote Schema for " + indexName);
    }

    public static String getCoreMetaInfo(String indexName) throws IOException
    {

        SolandraCoreInfo info = new SolandraCoreInfo(indexName);

        
        String schemaXML = readSchemaXML(info.coreName);
        long   maxId     = IndexManagerService.instance.getMaxId(indexName);
        int    numShards = CassandraIndexManager.getShardFromDocId(maxId);        
        
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n");
        sb.append("<solandraCore name=\""+indexName+"\" numSubIndexes=\""+(numShards+1)+"\" documentsPerSubIndex=\""+CassandraIndexManager.maxDocsPerShard+"\">\n");
        for(int i=0; i<=numShards; i++)
        {
            ByteBuffer subIndex = CassandraUtils.hashBytes((info.indexName + "~" + i).getBytes("UTF-8"));
    
            sb.append("  <subIndex name=\""+i+"\" token=\""+CassandraUtils.md5hash(subIndex)+"\">\n");
            
            Token<?> t = StorageService.getPartitioner().getToken(subIndex);
            List<InetAddress> addrs = Table.open(CassandraUtils.keySpace).getReplicationStrategy().getNaturalEndpoints(t);

            for(InetAddress addr : addrs)
            {
                sb.append("    <endpoint id=\""+addr+"\"/>\n");
            }
            sb.append("  </subIndex>\n");
        }
        
        sb.append("  <schema><![CDATA[\n");
        sb.append(schemaXML);
        sb.append("]]>\n  </schema>\n");
        
        sb.append("</solandraCore>\n");
        
        return sb.toString();
    }
    
    
    @Override
    public Collection<String> getCoreNames(SolrCore core)
    {
        return Arrays.asList(core.getName());
    }

}
