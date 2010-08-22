package solandra;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import lucandra.CassandraUtils;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;

public class SolandraCoreContainer extends CoreContainer {
    private static final Logger logger = Logger.getLogger(SolandraCoreContainer.class);
    public final static InstrumentedCache<String, SolrCore> cache = new InstrumentedCache<String, SolrCore>(1024);
    private final static QueryPath queryPath = new QueryPath("SI");
    private final static String columnName = "IDX";
    
    
    private final SolrConfig solrConfig;
    private final SolrCore   singleCore;
    
    public SolandraCoreContainer(SolrConfig solrConfig) {
        this.solrConfig = solrConfig;
        
        CoreDescriptor dcore = new CoreDescriptor(null, "", ".");
        
        singleCore = new SolrCore("", "/tmp", solrConfig, null, dcore);        
    }
    
    @Override
    public SolrCore getCore(String name) {
       
        if(name.equals(""))
            return singleCore;
        
        try {
            return SolandraCoreContainer.readSchema(name, solrConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

    public static SolrCore readSchema(String indexName, SolrConfig solrConfig) throws IOException {

        SolrCore core = cache.get(indexName);

        if (core == null) {
            // get from cassandra
            logger.debug("loading indexInfo for: "+ indexName);
            
            List<Row> rows = CassandraUtils.robustGet(indexName.getBytes(), queryPath, Arrays.asList(columnName.getBytes()), ConsistencyLevel.QUORUM);

            if (rows.isEmpty())
                throw new IOException("invalid index");

            if (rows.size() > 1)
                throw new IllegalStateException("More than one schema found for this index");

            InputStream stream = new ByteArrayInputStream(rows.get(0).cf.getColumn(columnName.getBytes()).value());

            IndexSchema schema = new IndexSchema(solrConfig, indexName, stream);
            core = new SolrCore(indexName, "/tmp",solrConfig,schema, null);
                   
            cache.put(indexName, core);
        }

        return core;
    }

    public static void writeSchema(String indexName, String schemaXml){
        RowMutation rm = new RowMutation(CassandraUtils.keySpace, indexName.getBytes());
        
        rm.add(new QueryPath("SI",null, columnName.getBytes()), schemaXml.getBytes(), new TimestampClock(System.currentTimeMillis()));

        CassandraUtils.robustInsert(Arrays.asList(rm), ConsistencyLevel.QUORUM);
    }
    
}
