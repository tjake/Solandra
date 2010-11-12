package solandra;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import lucandra.CassandraUtils;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolandraCoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.CoreContainer.Initializer;
import org.xml.sax.SAXException;

public class SolandraInitializer extends Initializer {

   
    @Override
    public CoreContainer initialize() throws IOException, ParserConfigurationException, SAXException {
                      
        CoreContainer cores = new SolandraCoreContainer(solrConfigFilename == null ? SolrConfig.DEFAULT_CONF_FILE : solrConfigFilename);
             
        //Startup cassandra
        CassandraUtils.startup();
        
        return cores;
    }
}
