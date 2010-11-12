package org.apache.solr.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import lucandra.CassandraUtils;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.xml.sax.SAXException;

public class SolandraCoreContainer extends CoreContainer {
    private static final Logger logger = Logger.getLogger(SolandraCoreContainer.class);
    private final static InstrumentedCache<String, SolrCore> cache = new InstrumentedCache<String, SolrCore>(1024);
    private final static QueryPath queryPath = new QueryPath(CassandraUtils.schemaInfoColumnFamily,CassandraUtils.schemaKeyBytes);
    public  final static Pattern shardPattern = Pattern.compile("^([^~]+)~?(\\d*)$");
    
    private final String     solrConfigFile;
    private final SolrCore   singleCore;
    
    public SolandraCoreContainer(String solrConfigFile) throws ParserConfigurationException, IOException, SAXException {
        this.solrConfigFile = solrConfigFile;
        
        SolrConfig     solrConfig = new SolrConfig(solrConfigFile);
        CoreDescriptor dcore = new CoreDescriptor(null, "", ".");
        
        singleCore = new SolrCore("", "/tmp", solrConfig, null, dcore);        
    }
    
    @Override
    public SolrCore getCore(String name) {
       
        logger.info("Loading Solandra core: "+name);
        
        SolrCore core = null;
        
        if(name.equals("")){
            core = singleCore;
        }else{
            
            Matcher m = shardPattern.matcher(name);
            if(!m.find())
                throw new RuntimeException("Invalid indexname: "+name);
            
            try {
                core = readSchema(m.group(1));
            } catch (IOException e) {
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
        
        if(core != null){
            synchronized(core){
                core.open();
            }
        }
     
        return core;
    }

    public static String  readSchemaXML(String indexName) throws IOException {
        List<Row> rows = CassandraUtils.robustRead(ByteBuffer.wrap((indexName+"/schema").getBytes()), queryPath, 
        										  Arrays.asList(CassandraUtils.schemaKeyBytes), 
        										  ConsistencyLevel.QUORUM);

        if (rows.isEmpty())
            throw new IOException("invalid index");

        if (rows.size() > 1)
            throw new IllegalStateException("More than one schema found for this index");

        if(rows.get(0).cf == null)
            throw new IOException("invalid index");
        
        ByteBuffer schema = rows.get(0).cf.getColumn(CassandraUtils.schemaKeyBytes).getSubColumn(CassandraUtils.schemaKeyBytes).value();
        return ByteBufferUtil.string(schema);
    }
    
    
    public synchronized SolrCore readSchema(String indexName) throws IOException, ParserConfigurationException, SAXException {

        SolrCore core = cache.get(indexName);

        if (core == null) {
            // get from cassandra
            logger.info("loading indexInfo for: "+ indexName);
            
            List<Row> rows = CassandraUtils.robustRead(ByteBuffer.wrap((indexName+"/schema").getBytes()), queryPath, 
            										  Arrays.asList(CassandraUtils.schemaKeyBytes), 
            										  ConsistencyLevel.QUORUM);

            if (rows.isEmpty())
                throw new IOException("invalid index");

            if (rows.size() > 1)
                throw new IllegalStateException("More than one schema found for this index");

            if(rows.get(0).cf == null)
                throw new IOException("invalid index");
            
            ByteBuffer buf = rows.get(0).cf.getColumn(CassandraUtils.schemaKeyBytes).getSubColumn(CassandraUtils.schemaKeyBytes).value();
            InputStream stream = new ByteArrayInputStream(buf.array(),buf.position(),buf.remaining());

            SolrConfig solrConfig = new SolrConfig(solrConfigFile);
            
            IndexSchema schema = new IndexSchema(solrConfig, indexName, stream);
            
            core = new SolrCore(indexName, "/tmp",solrConfig,schema, null);

//            //Something in solr 1.4.1 requires this inform
//            for(Map.Entry<String, SolrRequestHandler>  e : core.getRequestHandlers().entrySet())
//            {                
//                if(e.getValue() instanceof SolrCoreAware)
//                {             
//                    ((SolrCoreAware) e.getValue()).inform(core);
//                }
//            
//                if(e.getValue() instanceof ResourceLoaderAware ) {
//                    ((ResourceLoaderAware) e.getValue()).inform(core.getResourceLoader());
//                }          
//            }
            
            logger.debug("Loaded core from cassandra: "+indexName);
            
            cache.put(indexName, core);
        }

        return core;
    }

    public static void writeSchema(String indexName, String schemaXml){
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, ByteBuffer.wrap((indexName+"/schema").getBytes()));
        
        try {
        	
            rm.add(new QueryPath(CassandraUtils.schemaInfoColumnFamily,CassandraUtils.schemaKeyBytes, CassandraUtils.schemaKeyBytes), 
            		ByteBuffer.wrap(schemaXml.getBytes("UTF-8")), System.currentTimeMillis());
            
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        CassandraUtils.robustInsert(ConsistencyLevel.ONE, rm);
        
        logger.debug("Wrote Schema for "+indexName);
    }

    @Override
    public Collection<String> getCoreNames(SolrCore core) {
        return Arrays.asList(core.getName());
    }
    
}
