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

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
    
public class SolandraReopenComponent extends SearchComponent {

    
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
    }

    public void process(ResponseBuilder rb) throws IOException {
        
       
    }

}
