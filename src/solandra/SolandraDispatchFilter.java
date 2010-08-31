package solandra;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.SolrDispatchFilter;

public class SolandraDispatchFilter extends SolrDispatchFilter {

    @Override
    protected Initializer createInitializer() {
        SolandraInitializer init =  new SolandraInitializer();
        
        return init;
    }

    @Override
    protected void execute(HttpServletRequest req, SolrRequestHandler handler, SolrQueryRequest sreq, SolrQueryResponse rsp) {
        
        String path = req.getServletPath();
        if( req.getPathInfo() != null ) {
          // this lets you handle /update/commit when /update is a servlet
          path += req.getPathInfo();
        }
        if( pathPrefix != null && path.startsWith( pathPrefix ) ) {
          path = path.substring( pathPrefix.length() );
        }
        // check for management path
        String alternate = cores.getManagementPath();
        if (alternate != null && path.startsWith(alternate)) {
          path = path.substring(0, alternate.length());
        }
        
        int idx = path.indexOf( "/", 1 );
        if( idx > 1 ) {
          // try to get the corename as a request parameter first
          sreq.getContext().put("solandra-index", path.substring( 1, idx ));
        }
        
        super.execute(req, handler, sreq, rsp);
    }

}
