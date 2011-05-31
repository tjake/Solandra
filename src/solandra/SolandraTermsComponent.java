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

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.TermsComponent;

public class SolandraTermsComponent extends TermsComponent
{
    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        
        if( SolandraComponent.prepare(rb) )
        {
            ModifiableSolrParams params = new ModifiableSolrParams(rb.req.getParams());
            
            if(params.get(ShardParams.SHARDS_QT) == null)
            {
                params.add(ShardParams.SHARDS_QT, "/terms");
            }
            
            rb.req.setParams(params);
        }
        
        super.prepare(rb);
    }
}
