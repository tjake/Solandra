/**
 * 
 */
package lucandra;

import java.io.UnsupportedEncodingException;

/**
 * @author Todd Nine
 *
 */
public class ByteHelper {
	

	/**
	 * Get UTF 8 bytes of the string.  If it's empty null will be returned
	 * @param value
	 * @return
	 */
	public static byte[] getBytes(String value){
		if(value == null){
			return new byte[]{};
		}
		
		try {
			return value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			//shouldn't happen
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Get the string value in utf 8 for the bytes
	 * @param value
	 * @return
	 */
	public static String getString(byte[] value){
		try {
			return new String(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			//should never happen
			throw new RuntimeException(e);
		}
	}
}
