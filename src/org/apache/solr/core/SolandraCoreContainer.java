package org.apache.solr.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lucandra.CassandraUtils;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.apache.solr.schema.IndexSchema;

public class SolandraCoreContainer extends CoreContainer {
    private static final Logger logger = Logger.getLogger(SolandraCoreContainer.class);
    private final static InstrumentedCache<String, SolrCore> cache = new InstrumentedCache<String, SolrCore>(1024);
    private final static QueryPath queryPath = new QueryPath("SI");
    private final static String columnName = "IDX";
    public  final static Pattern shardPattern = Pattern.compile("^([^~]+)~?(\\d*)$");
    
    private final SolrConfig solrConfig;
    private final SolrCore   singleCore;
    
    public SolandraCoreContainer(SolrConfig solrConfig) {
        this.solrConfig = solrConfig;
        
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
                core = SolandraCoreContainer.readSchema(m.group(1), solrConfig);
            } catch (IOException e) {
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
        List<Row> rows = CassandraUtils.robustGet(indexName.getBytes(), queryPath, Arrays.asList(columnName.getBytes()), ConsistencyLevel.QUORUM);

        if (rows.isEmpty())
            throw new IOException("invalid index");

        if (rows.size() > 1)
            throw new IllegalStateException("More than one schema found for this index");

        if(rows.get(0).cf == null)
            throw new IOException("invalid index");
        
        return new String(rows.get(0).cf.getColumn(columnName.getBytes()).value(),"UTF-8");   
    }
    
    
    public static synchronized SolrCore readSchema(String indexName, SolrConfig solrConfig) throws IOException {

        SolrCore core = cache.get(indexName);

        if (core == null) {
            // get from cassandra
            logger.info("loading indexInfo for: "+ indexName);
            
            List<Row> rows = CassandraUtils.robustGet(indexName.getBytes(), queryPath, Arrays.asList(columnName.getBytes()), ConsistencyLevel.QUORUM);

            if (rows.isEmpty())
                throw new IOException("invalid index");

            if (rows.size() > 1)
                throw new IllegalStateException("More than one schema found for this index");

            if(rows.get(0).cf == null)
                throw new IOException("invalid index");
            
            InputStream stream = new ByteArrayInputStream(rows.get(0).cf.getColumn(columnName.getBytes()).value());

            IndexSchema schema = new IndexSchema(solrConfig, indexName, stream);
            
            core = new SolrCore(indexName, "/tmp",solrConfig,schema, null);
                 
            logger.debug("Loaded core from cassandra: "+indexName);
            
            cache.put(indexName, core);
        }

        return core;
    }

    public static void writeSchema(String indexName, String schemaXml){
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, indexName.getBytes());
        
        try {
            rm.add(new QueryPath("SI",null, columnName.getBytes()), schemaXml.getBytes("UTF-8"), new TimestampClock(System.currentTimeMillis()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        CassandraUtils.robustInsert(Arrays.asList(rm), ConsistencyLevel.ONE);
        
        logger.debug("Wrote Schema for "+indexName);
    }

    @Override
    public Collection<String> getCoreNames(SolrCore core) {
        return Arrays.asList(core.getName());
    }
    
}
