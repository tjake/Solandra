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
package lucandra.dht;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.Token;

public class DecoratedKey<T extends Token> extends org.apache.cassandra.db.DecoratedKey<T>
{

    public DecoratedKey(T token, ByteBuffer key)
    {
        super(token, key);
       
    }

    public int compareTo(org.apache.cassandra.db.DecoratedKey other)
    {
        
        int cmp = token.compareTo(other.token);
        
        if(cmp == 0)
        {
            if(key == null && other.key == null)
            {
                return 0;
            }
            
            if(key == null)
            {
                return 1;
            }
            
            if(other.key == null)
            {
                return -1;
            }
            
            
            return key.compareTo(other.key);
        }
        
        return cmp;
    }

    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        DecoratedKey other = (DecoratedKey) obj;
        
        if(token.equals(other.token))
        {
            if(key == null && other.key == null)
            {
                return true;
            }
            
            if(key == null || other.key == null)
            {
                return false;              
            }
            
            return key.equals(other.key);
        }
        
        return false;
    }

    public int hashCode()
    {    
         return token.hashCode() + (key == null ? 0 : key.hashCode());
    }

}
