package lucandra;

public class BitSetUtils {

    private static byte[] bitMasks = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte)0x80};
    
    public static byte[] create(int numberOfBits){
        int size = (int)Math.ceil(numberOfBits/8.0);
        return new byte[size];
    }
    
    public static boolean get(byte[] bytes, int n){
        int p = n/8;        
        int offset = n % 8;
        
        if(bytes.length <= p)
            throw new ArrayIndexOutOfBoundsException(p);
        
        byte i = bytes[p];
        
        return (i & bitMasks[offset]) != 0x0;        
    }
    
    public static void set(byte[] bytes, int n){
        int p = n/8;        
        int offset = n % 8;
        
        if(bytes.length <= p)
            throw new ArrayIndexOutOfBoundsException(p);
        
        bytes[p] |= bitMasks[offset];    
    }
    
    public static byte[] or(byte[] a, byte[] b){
        if(a.length != b.length)
            throw new IllegalStateException("a != b");
        
        for(int i=0; i<a.length; i++){
            a[i] |= b[i];
        }
        
        return a;
    }
    
}
