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
