package solandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.core.SolandraCoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.SolrDispatchFilter;

public class SolandraDispatchFilter extends SolrDispatchFilter {

    private static final String schemaPrefix = "/schema"; 

    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        String indexName = "";
        String path = req.getServletPath();
        if (req.getPathInfo() != null) {
            // this lets you handle /update/commit when /update is a servlet
            path += req.getPathInfo();
        }
        if (path.startsWith(schemaPrefix)) {
            path = path.substring(schemaPrefix.length());

            // otherwise, we should find a index from the path
            int idx = path.indexOf("/", 1);
            if (idx > 1) {
                // try to get the index as a request parameter first
                indexName = path.substring(1, idx);
            } else {
                indexName = path.substring(1);
            }

            // REST
            String method = req.getMethod().toUpperCase();

            if (method.equals("GET")) {
                try {
                    String schema = SolandraCoreContainer.readSchemaXML(indexName);
                    response.setContentType("text/xml");
                    PrintWriter out = resp.getWriter();

                    out.print(schema);

                } catch (IOException e) {
                    resp.sendError(404);
                }

                return;
            }

            if (method.equals("POST") || method.equals("PUT")) {
                try {
                    
                    
                    BufferedReader rd = new BufferedReader(new InputStreamReader(req.getInputStream()));
                    String line;
                    String xml = "";
                    while ((line = rd.readLine()) != null) {
                        xml += line+"\n";
                    }
                    
                    
                    SolandraCoreContainer.writeSchema(indexName, xml);

                } catch (IOException e) {
                    resp.sendError(500);
                }
                return;
            }
        }

        
        super.doFilter(request, response, chain);
    }
    
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
       
        
        int idx = path.indexOf( "/", 1 );
        if( idx > 1 ) {
          // try to get the corename as a request parameter first
          sreq.getContext().put("solandra-index", path.substring( 1, idx ));
        }
        
        super.execute(req, handler, sreq, rsp);
    }

}
