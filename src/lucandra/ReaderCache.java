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

import java.util.Map;
import java.util.UUID;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.OpenBitSet;

import com.google.common.collect.MapMaker;

public class ReaderCache
{
    public final String indexName;
    public final Map<Integer, Document> documents;
    public final Map<Term, LucandraTermEnum> termEnum;
    public final Map<String, byte[]>  fieldNorms;
    public final OpenBitSet docHits;
    public final Object fieldCacheKey;
    
    public ReaderCache(String indexName)
    {
        this.indexName = indexName;
        
        documents  = new MapMaker().makeMap();
        termEnum   = new MapMaker().makeMap();
        fieldNorms = new MapMaker().makeMap();
        docHits    = new OpenBitSet(CassandraUtils.maxDocsPerShard);
        
        fieldCacheKey = UUID.randomUUID();
    }
    
    
}
