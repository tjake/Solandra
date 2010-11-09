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
