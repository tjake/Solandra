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

import java.io.*;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.solr.core.SolandraCoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.request.*;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.cache.Method;

public class SolandraDispatchFilter extends SolrDispatchFilter
{
    private static final String schemaPrefix = "/schema";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException
    {

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        String indexName = "";
        String resourceName = "";
        String path = req.getServletPath();

        if (req.getPathInfo() != null)
        {
            // this lets you handle /update/commit when /update is a servlet
            path += req.getPathInfo();
        }

        if (path.startsWith(schemaPrefix))
        {
            path = path.substring(schemaPrefix.length());

            // otherwise, we should find a index from the path
            int idx = path.indexOf("/", 1);
            if (idx > 1)
            {
                // try to get the index as a request parameter first
                indexName = path.substring(1, idx);
                resourceName = path.substring(idx+1);
            }
            else
            {
                indexName = path.substring(1);
            }

            // REST
            String method = req.getMethod().toUpperCase();

            if (method.equals("GET"))
            {
                try
                {
                    String resource = "";
                    
                    if(resourceName.isEmpty() || resourceName.equalsIgnoreCase("schema.xml"))
                    {
                        resource = SolandraCoreContainer.getCoreMetaInfo(indexName);
                        response.setContentType("text/xml");
                    }
                    else
                    {
                        resource = ByteBufferUtil.string(SolandraCoreContainer.readCoreResource(indexName, resourceName));
                    }
                        
                    PrintWriter out = resp.getWriter();
                    out.print(resource);

                }
                catch (IOException e)
                {
                    resp.sendError(404,e.toString());
                }

                return;
            }

            if (method.equals("POST") || method.equals("PUT"))
            {
                try
                {

                    BufferedReader rd = new BufferedReader(new InputStreamReader(req.getInputStream()));
                    String line;
                    String resource = "";
                    while ((line = rd.readLine()) != null)
                    {
                        resource += line + "\n";
                    }

                    if(resourceName.isEmpty() || resourceName.equalsIgnoreCase("schema.xml"))
                    {
                        SolandraCoreContainer.writeSchema(indexName, resource);
                    }
                    else
                    {
                        SolandraCoreContainer.writeCoreResource(indexName, resourceName, resource);
                    }
                }
                catch (IOException e)
                {
                    resp.sendError(500);
                }
                return;
            }
        }

        SolandraCoreContainer.activeRequest.set(req);

        super.doFilter(request, response, chain);
    }

    @Override
    protected Initializer createInitializer()
    {
        SolandraInitializer init = new SolandraInitializer();

        return init;
    }
    
    @Override
    protected void execute(HttpServletRequest req, SolrRequestHandler handler, SolrQueryRequest sreq,
            SolrQueryResponse rsp)
    {

        String path = req.getServletPath();
        if (req.getPathInfo() != null)
        {
            // this lets you handle /update/commit when /update is a servlet
            path += req.getPathInfo();
        }
        if (pathPrefix != null && path.startsWith(pathPrefix))
        {
            path = path.substring(pathPrefix.length());
        }

        int idx = path.indexOf("/", 1);
        if (idx > 1)
        {
            // try to get the corename as a request parameter first
            sreq.getContext().put("solandra-index", path.substring(1, idx));
        }

        super.execute(req, handler, sreq, rsp);
    }

   

    private void writeResponse(SolrQueryResponse solrRsp, ServletResponse response, QueryResponseWriter responseWriter,
            SolrQueryRequest solrReq, Method reqMethod) throws IOException
    {
        if (solrRsp.getException() != null)
        {
            sendError((HttpServletResponse) response, solrRsp.getException());
        }
        else
        {
            // Now write it out
            response.setContentType(responseWriter.getContentType(solrReq, solrRsp));
            if (Method.HEAD != reqMethod)
            {
                if (responseWriter instanceof BinaryQueryResponseWriter)
                {
                    BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
                    binWriter.write(response.getOutputStream(), solrReq, solrRsp);
                }
                else
                {
                    PrintWriter out = response.getWriter();
                    responseWriter.write(out, solrReq, solrRsp);

                }
            }
            // else http HEAD request, nothing to write out, waited this long
            // just to get ContentType
        }
    }

}
