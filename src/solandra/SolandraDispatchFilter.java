package solandra;

import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.servlet.SolrDispatchFilter;

public class SolandraDispatchFilter extends SolrDispatchFilter {

    @Override
    protected Initializer createInitializer() {
        SolandraInitializer init =  new SolandraInitializer();
        
        return init;
    }

}
