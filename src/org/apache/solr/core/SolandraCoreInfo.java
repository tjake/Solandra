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
package org.apache.solr.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolandraCoreInfo
{
    
    public final String  coreName;
    public final String  indexName;
    public final Integer shard;
    public final boolean bulk;
    public final boolean isShard;
    
    public  final static Pattern shardPattern = Pattern.compile("^(~?)([^\\.~]+)(\\.?[^~]*)~?(\\d*)$");

    
    public SolandraCoreInfo(String indexString)
    {
        Matcher m = shardPattern.matcher(indexString);
        
        if(!m.find())
            throw new RuntimeException("Invalid indexname: "+indexString);
        
        bulk      = m.group(1).isEmpty() ? false : true;
        coreName  = m.group(2);
        indexName = m.group(3).isEmpty() ? coreName : coreName+m.group(3);
        shard     = m.group(4).isEmpty() ? 0 : Integer.valueOf(m.group(4));
        isShard   = !m.group(4).isEmpty();
    }
    

}
