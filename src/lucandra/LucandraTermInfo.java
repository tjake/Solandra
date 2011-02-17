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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;

public class LucandraTermInfo implements Comparable<LucandraTermInfo>
{
    
    
    public final int     docId;
    public final boolean hasNorm;
    public final boolean hasPositions;
    public final boolean hasOffsets;
    
    public final int[]   positions;
    public final int[]   offsets;
    public final int     freq;
    public final Byte    norm;
    
    
    public LucandraTermInfo(int docId, Map<ByteBuffer, List<Number>> data)
    {
        this.docId = docId;
        
        boolean hasNorm_ = false;
        Byte    norm_    = null;
        
        boolean hasPositions_ = false;
        int[]  positions_     = null;
        
        boolean hasOffsets_   = false;
        int[]  offsets_   = null;
        
        int     freq_         = -1;
        
        
        for (Map.Entry<ByteBuffer, List<Number>> e : data.entrySet())
        {
            assert e.getKey() != null;
            
            if(e.getKey().equals(CassandraUtils.normsKeyBytes))
            {
                hasNorm_ = true;    
                norm_ = e.getValue().get(0).byteValue();
            }
          
            else if(e.getKey().equals(CassandraUtils.positionVectorKeyBytes))
            {
                List<Number> p = e.getValue();
                
                positions_ = new int[p.size()];
                for(int i=0; i<positions_.length; i++)
                    positions_[i] = p.get(i).intValue();
                
                hasPositions_ = positions_.length > 0 ? true : false;
            }
               
            else if(e.getKey().equals(CassandraUtils.offsetVectorKeyBytes))
            {
                List<Number> o = e.getValue();
                
                offsets_ = new int[o.size()];
                for(int i=0; i<offsets_.length; i++)
                    offsets_[i] = o.get(i).intValue();

                hasOffsets_ = offsets_.length > 0 ? true : false;
            }
            
            else if(e.getKey().equals(CassandraUtils.termFrequencyKeyBytes))
            {
                List<Number> value = e.getValue();
                
                freq_ = value.size() == 0 ? 0 : value.get(0).intValue();
            }
            
            else
            {
                try
                {
                    throw new IllegalArgumentException(ByteBufferUtil.string(e.getKey()));
                }
                catch (CharacterCodingException e1)
                {
                    throw new IllegalArgumentException(e.getKey().toString());
                }
            }
        }
        
        if(freq_ < 0)
            throw new IllegalArgumentException("term freq is < 0");
    
        if(hasPositions_ && freq_ != positions_.length)
            throw new IllegalArgumentException("freq != position count: "+freq_+" vs "+positions_.length);
            
        //Set final vars
        freq         = freq_;
        hasNorm      = hasNorm_;
        norm         = norm_;
        hasPositions = hasPositions_;
        positions    = positions_;
        hasOffsets   = hasOffsets_;
        offsets      = offsets_;
    }

    public LucandraTermInfo(int docId, ByteBuffer bytes_)
    {
        this.docId = docId;
        
        ByteBuffer bytes = bytes_.duplicate(); //don't mutate the original
        
        byte flags = bytes.get();
           
        hasNorm      = (flags & 1) == 1;
        hasPositions = (flags & 2) == 2;
        hasOffsets   = (flags & 4) == 4;
        
        freq = CassandraUtils.mreadVInt(bytes);
        
        norm = hasNorm ? bytes.get() : null;
        
        int[] positions_ = null;
        if(hasPositions)
        {
            positions_ = new int[freq];
        
            for(int i=0; i<freq; i++)
                positions_[i] = CassandraUtils.mreadVInt(bytes);
             
        }
        
        positions = positions_;
        
        int[] offsets_ = null;
        if(hasOffsets)
        {
            int len = CassandraUtils.mreadVInt(bytes);
            offsets_ = new int[len];
            
            for(int i=0; i<len; i++)
                offsets_[i] = CassandraUtils.mreadVInt(bytes);          
        }
        
        offsets = offsets_;
    }
    
    
    public ByteBuffer serialize()
    {
        //         flags, freq, norm, pos, numoff, off 
        int size = 1 + 4 + (hasNorm ? 1 : 0) + (hasPositions ? positions.length*4 : 0) + (hasOffsets ? positions.length*4+4 : 0);
        ByteBuffer r = ByteBuffer.allocate(size);
        
        //store the initial content flags in the inital byte
        byte flags = 0;
        if(hasNorm)
            flags |= 1;
        
        if(hasPositions)
            flags |= 2;
        
        if(hasOffsets)
            flags |= 4;
        
        r.put(flags);
        r.put(CassandraUtils.writeVInt(freq));
        
        
        if(hasNorm)
            r.put(norm);
        
        if(hasPositions)
        {
            for(int i=0; i<positions.length; i++)
            {
                r.put(CassandraUtils.writeVInt(positions[i]));
            }
        }
        
        if(hasOffsets)
        {
            r.put(CassandraUtils.writeVInt(offsets.length));
            
            for(int i=0; i<offsets.length; i++)
            {
                r.put(CassandraUtils.writeVInt(offsets[i]));
            }
        }
  
        r.flip();
  
        return r;
    }

    public int compareTo(LucandraTermInfo o)
    {
        if(this.docId < o.docId)
            return -1;
        
        if(this.docId > o.docId)
            return 1;
        
        return 0;
    }
    
    
    
}
