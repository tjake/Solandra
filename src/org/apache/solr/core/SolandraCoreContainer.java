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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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

import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.solr.common.util.ConcurrentLRUCache;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import solandra.SolandraResourceLoader;


public class SolandraCoreContainer extends CoreContainer
{
    public  final static ThreadLocal<SolandraCoreInfo>        coreInfo      = new InheritableThreadLocal<SolandraCoreInfo>();
    public  final static ThreadLocal<HttpServletRequest>      activeRequest = new InheritableThreadLocal<HttpServletRequest>();
    public  final static int                                  CAPACITY      = 1024;

    
    private static final Logger                              logger    = Logger.getLogger(SolandraCoreContainer.class);

    private static final ConcurrentLRUCache<String, SolrCore> cache  = new ConcurrentLRUCache<String, SolrCore>(CAPACITY, CAPACITY/2);
   
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
        ByteBuffer schema = readCoreResource(indexName, CassandraUtils.schemaKey);
        
        //Schema resource not found for the core
        if (schema == null)
        {
        	throw new IOException(String.format("invalid core '%s'", indexName));
        }
        
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
                logger.debug("loading index schema for: " + indexName);
            
            ByteBuffer buf = readCoreResource(indexName, CassandraUtils.schemaKey);
            
            //Schema resource not found for the core
            if (buf == null)
            {
            	throw new IOException(String.format("invalid core '%s'", indexName));
            }
            
            InputStream stream = new ByteArrayInputStream(ByteBufferUtil.getArray(buf));

            SolrResourceLoader resourceLoader = new SolandraResourceLoader(indexName, null);
            SolrConfig solrConfig = new SolrConfig(resourceLoader, solrConfigFile, null);

            IndexSchema schema = new IndexSchema(solrConfig, indexName, new InputSource(stream));

            core = new SolrCore(indexName, "/tmp", solrConfig, schema, null);

            if (logger.isDebugEnabled())
            	logger.debug("Loaded core from cassandra: " + indexName);

            cache.put(indexName, core);
        }

        return core;
    }

    public static void writeSchema(String indexName, String schemaXml) throws IOException
    {        
        writeCoreResource(indexName, CassandraUtils.schemaKey, schemaXml);

        logger.info("Wrote Schema for " + indexName);
        
        //remove any existing cores
        cache.remove(indexName);
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
    
	public static ByteBuffer readCoreResource(String coreName, String resourceName) throws IOException
	{
		
		if (coreName == null || resourceName == null)
		{
			return null;
		}
		
		ByteBuffer resourceValue = null;

		ByteBuffer coreKey = CassandraUtils.hashKeyBytes(coreName.getBytes("UTF-8"),
                CassandraUtils.delimeterBytes, "resources".getBytes("UTF-8"));
       
		
		ByteBuffer coreNameBytes = ByteBufferUtil.bytes(coreName);
		ByteBuffer resourceNameBytes = ByteBufferUtil.bytes(resourceName);

		QueryPath queryPath = new QueryPath(
				CassandraUtils.schemaInfoColumnFamily,
				coreNameBytes);
		
		List<Row> rows = CassandraUtils.robustRead(
				coreKey,
				queryPath,
				Arrays.asList(resourceNameBytes),
				ConsistencyLevel.QUORUM);

		if (rows.isEmpty())
			throw new IOException("invalid core: " + coreName);

		if (rows.size() > 1)
		{
			throw new IllegalStateException(
					String.format("More than one resource named '%s' found for core '%s'",
							resourceName, coreName));
		}

		/*
		 * Named resource does exist for the core, return its value.
		 * Otherwise return null.
		 */
		if (rows.get(0).cf != null)
		{
			resourceValue = rows.get(0).cf
				.getColumn(coreNameBytes)
				.getSubColumn(resourceNameBytes).value();
		}
		else
		{
		    logger.info("row was marked empty: "+rows);
		}
		
		return resourceValue;
	}
	
    public static void writeCoreResource(String coreName, String resourceName, String resourceValue) throws IOException
    {

		//TODO (jschmidt): do some parameter validation (not null etc.)
		
		if (logger.isDebugEnabled())
		{
			logger.debug(String.format("Writing resource '%s' to core '%s'", resourceName, coreName));
			logger.debug(resourceValue);
		}
		
		
		ByteBuffer coreKey = CassandraUtils.hashKeyBytes(coreName.getBytes("UTF-8"),
                 CassandraUtils.delimeterBytes, "resources".getBytes("UTF-8"));
		
		RowMutation rm = new RowMutation(CassandraUtils.keySpace, coreKey);

		ByteBuffer coreNameBytes = ByteBufferUtil.bytes(coreName);
        ByteBuffer resourceNameBytes = ByteBufferUtil.bytes(resourceName);
        
		QueryPath queryPath = new QueryPath(
				CassandraUtils.schemaInfoColumnFamily,
				coreNameBytes,
				resourceNameBytes);

        rm.add(
        		queryPath,
        		ByteBufferUtil.bytes(resourceValue),
        		System.currentTimeMillis());

        CassandraUtils.robustInsert(ConsistencyLevel.QUORUM, rm);
        
        if (logger.isDebugEnabled())
        {
        	logger.debug(String.format("wrote resource '%s' to core '%s'",
        			resourceName, coreName));	
        }       
    }
}
