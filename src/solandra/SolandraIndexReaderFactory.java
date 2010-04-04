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

import lucandra.CassandraUtils;
import lucandra.IndexReader;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.thrift.transport.TTransportException;

public class SolandraIndexReaderFactory extends IndexReaderFactory {  
        
    private String indexName;
    private String cassandraHost;
    private Integer cassandraPort;
    private Boolean cassandraFramed;
    
    
    public void init(NamedList args){
        super.init(args);
        
        indexName = (String)args.get("indexName");
            
        if(indexName == null || indexName.length() == 0)
            throw new SolrException(ErrorCode.NOT_FOUND, "<str name=\"indexName\">example</str>  tag required");
        
        
        cassandraHost = (String)args.get("cassandraHost");
        
        if(cassandraHost == null || cassandraHost.length() == 0)
            throw new SolrException(ErrorCode.NOT_FOUND, "<str name=\"cassandraHost\">localhost</str>  tag required");
        
        
        cassandraPort = (Integer)args.get("cassandraPort");
        
        if(cassandraPort == null)
            throw new SolrException(ErrorCode.NOT_FOUND, "<int name=\"cassandraPort\">9160</int>  tag required");
        
        
        cassandraFramed = (Boolean)args.get("cassandraFramed");
        if(cassandraFramed == null)
            cassandraFramed = false;
        
    }
    
    @Override
    public IndexReader newReader(Directory indexDir, boolean readOnly) throws IOException {
        
        Cassandra.Iface client;
       
        try{
            client = CassandraUtils.createConnection(cassandraHost,cassandraPort,cassandraFramed);       
        }catch(TTransportException e){
            throw new IOException(e.getLocalizedMessage());
        }
                
        return new IndexReader(indexName, client);        
    }

}
