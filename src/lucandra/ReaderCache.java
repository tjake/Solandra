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
package lucandra;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import lucandra.cluster.CassandraIndexManager;

import com.google.common.collect.MapMaker;

import org.apache.lucene.document.Document;
import org.apache.lucene.util.OpenBitSet;

public class ReaderCache
{
    public final String indexName;
    public final Map<Integer, Document> documents;
    public final TermCache termCache;
    public final Map<String, byte[]>  fieldNorms;
    public final OpenBitSet docHits;
    public final Object fieldCacheKey;
    
    public ReaderCache(String indexName) throws IOException
    {
        this.indexName = indexName;
        
        documents           = new MapMaker().makeMap();
        termCache           = new TermCache(indexName);
        fieldNorms          = new MapMaker().makeMap();
        docHits             = new OpenBitSet(CassandraIndexManager.maxDocsPerShard);
        
        fieldCacheKey = UUID.randomUUID();
    }
    
    
}
