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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

public class SolandraFieldSelector implements FieldSelector {

    List<Integer> otherDocsToCache;
    List<ByteBuffer> fieldNames;
    
    public SolandraFieldSelector(List<Integer> otherDocsToCache, List<ByteBuffer> fieldNames){
        this.otherDocsToCache = otherDocsToCache;
        this.fieldNames = fieldNames;
    }
    
    public List<Integer> getOtherDocsToCache(){
        return otherDocsToCache;
    }
    
    public List<ByteBuffer> getFieldNames(){
        return fieldNames;
    }
    
    public FieldSelectorResult accept(String fieldName) {
        // TODO Auto-generated method stub
        return null;
    }

}
