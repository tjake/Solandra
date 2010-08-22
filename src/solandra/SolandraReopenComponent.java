/**
 * Copyright 2010 T Jake Luciani
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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import lucandra.CassandraUtils;
import lucandra.cluster.AbstractIndexManager;
import lucandra.cluster.RedisIndexManager;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;
import org.apache.lucene.document.FieldSelector;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
    
public class SolandraReopenComponent extends SearchComponent {

    private static final Logger logger = Logger.getLogger(SolandraReopenComponent.class);
    private final RedisIndexManager indexManager;
    private final Random random;
   
    public SolandraReopenComponent(){
        indexManager = new RedisIndexManager(CassandraUtils.service);
        random       = new Random(System.currentTimeMillis());
    }
    
    public String getDescription() {
       return "Reopens Lucandra readers";
    }

  
    public String getSource() {        
        return null;
    }

    
    public String getSourceId() {
        return null;
    }

    public String getVersion() {
        return "1.0";
    }

    public void prepare(ResponseBuilder rb) throws IOException {
            
        //Fixme: this is quite hacky, but the only way to make it work
        //without altering solr. neeeds reopen!
        rb.req.getSearcher().getIndexReader().directory();      
        
        String indexName = rb.req.getCore().getName();
        
        if(indexName.equals(""))
            return;
        
        //re-create new request object
        // rb.req = new LocalSolrQueryRequest(newCore, rb.req.getParams());
        
        if(rb.shards == null){
        
            //find number of shards
            int docId =  indexManager.getCurrentDocId(indexName);
        
            int numShards = AbstractIndexManager.getShardFromDocId(docId);
               
            //assign shards
            String[] shards = new String[numShards+1];
            
            for(int i=0; i<=numShards; i++){
                String subIndex = indexName + i;
                Token t = StorageService.getPartitioner().getToken(subIndex.getBytes());
                List<InetAddress> addrs = StorageService.instance.getReplicationStrategy(CassandraUtils.keySpace).getNaturalEndpoints(t, CassandraUtils.keySpace);
            
                //pick a replica at random
                if(addrs.isEmpty())
                    throw new IOException("can't locate index");
                
                
                InetAddress addr = addrs.get(random.nextInt(addrs.size())); 
                String shard = addr.getHostAddress() + ":8983/solr";

                logger.info("Adding shard("+indexName+"): "+shard);
            }
            
            //assign to shards
            rb.shards = shards;
            
        }
    }

    
    public void process(ResponseBuilder rb) throws IOException {
        
        DocList list = rb.getResults().docList;
    
        DocIterator it = list.iterator();
           
        List<Integer> docIds = new ArrayList<Integer>(list.size());
        while(it.hasNext())
            docIds.add(it.next());
              
        logger.debug("Fetching "+docIds.size()+" Docs");    

        
        if(docIds.size() > 0){
            
            List<byte[]> fieldFilter = null;
            Set<String> returnFields = rb.rsp.getReturnFields();
            if(returnFields != null) {
              
              // copy return fields list
              fieldFilter = new ArrayList<byte[]>(returnFields.size());
              for(String field : returnFields){
                  fieldFilter.add(field.getBytes());
              }
              
              
              // add highlight fields
              SolrHighlighter highligher = rb.req.getCore().getHighlighter();
              if(highligher.isHighlightingEnabled(rb.req.getParams())) {
                for(String field: highligher.getHighlightFields(rb.getQuery(), rb.req, null)) 
                  if(!returnFields.contains(field))
                      fieldFilter.add(field.getBytes());        
              }
              // fetch unique key if one exists.
              SchemaField keyField = rb.req.getSearcher().getSchema().getUniqueKeyField();
              if(null != keyField)
                  if(!returnFields.contains(keyField))
                      fieldFilter.add(keyField.getName().getBytes());  
            }
                      
            FieldSelector selector = new SolandraFieldSelector(docIds,fieldFilter);
        
            rb.req.getSearcher().getReader().document(docIds.get(0), selector);    
        }  
    }
}
