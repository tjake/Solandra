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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class VIntType extends AbstractType<BigInteger> {

    public static final VIntType instance = new VIntType();

    @Override
    public String getString(ByteBuffer bytes) {
        return Integer.toString(CassandraUtils.readVInt(bytes));          
    }

    @Override
    public ByteBuffer fromString(String source) {
        return ByteBuffer.wrap(CassandraUtils.writeVInt(new Integer(source)));
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
        if(null == o1){
            if(null == o2) return 0;
            else return -1;
        }
        if(null == o2) return 1;
        
        if(0 == o1.remaining()){
            if(0 == o2.remaining()) return 0;
            else return -1;
        }
        if(0 == o2.remaining()) return 1;
        
        //in vints, length equates to value size
        if(o1.remaining() < o2.remaining())
            return -1;
        
        if(o2.remaining() < o1.remaining())
            return 1;       
        
        int i1 = CassandraUtils.readVInt(o1);
        int i2 = CassandraUtils.readVInt(o2);
        
        if(i1 == i2) return 0;
        
        return i1 < i2 ?  -1 : 1;
    }

    @Override
    public BigInteger compose(ByteBuffer bytes)
    {
        return new BigInteger(ByteBufferUtil.getArray(bytes));
    }

    @Override
    public ByteBuffer decompose(BigInteger value)
    {
        return ByteBuffer.wrap(value.toByteArray());
    }

    public String toString(BigInteger bigInteger) {
        return bigInteger.toString();
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        
    }

    public Class<BigInteger> getType() {
        return BigInteger.class;
    }

    public boolean isSigned() {
        return true;
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public int getPrecision(BigInteger obj) {
        return  obj.toString().length();
    }

    public int getScale(BigInteger obj) {
        return 0;
    }

    public int getJdbcType() {
        return Types.BIGINT;
    }

    public boolean needsQuotes()
    {
        return false;
    }

}
