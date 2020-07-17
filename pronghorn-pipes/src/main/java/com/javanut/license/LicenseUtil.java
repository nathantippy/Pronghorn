package com.javanut.license;


import java.security.SecureRandom;
import java.util.Base64;
import java.util.Calendar;

public class LicenseUtil {

	public final static byte PRODUCT_BASIC = 0B1000;
	public final static byte PRODUCT_PRO   = 0B1110;

	public static final String TRIAL_VERSION_PREFIX = "trial-version-"; //followed by YYYY-MM-DD
	
    public static byte thisQuarter() {  //works up to 2084  
      Calendar instance = Calendar.getInstance();
      
	  return computeQuarter(instance);
    }


	public static byte computeQuarter(Calendar instance) {
		int year = instance.get(Calendar.YEAR)-2020;
		  if (year >= 64) {
		  	year = 63;
		  }
		  
		  int quarter =  instance.get(Calendar.MONTH)/3;
		  assert(quarter<=4); 
		
		  return (byte) ((year<<2) | quarter);
	}

   public static byte[] extractEncodedData(String key) {
        
        StringBuilder plainText = new StringBuilder();
        
        int dashCount = 0;
        for(char c: key.toCharArray()) {
            if ('-'!=c) {
        		plainText.append(c);
        	} else {
        		dashCount++;
        	}
        }
        if ((3!=dashCount) || (32!=plainText.length()) ) {
        	return null;
        }
        
        byte[] result = new byte[(plainText.length()-1)>>3];  
		byte[] data = Base64.getDecoder().decode(plainText.toString());
        
        for(int c=0; c<data.length; c++) {
	        	result[c>>3] = (byte)( result[c>>3] | ((1<<(c & 0x7)) & (int) data[c]));
	            // System.out.println("decode: "+c+"   "+ ((1<<(c & 0x7)) & (int) data[c]) );
        }
    	
    	return result;
    }


	public static String generateNewKey(byte[] encodedData, byte[] invalidChars) {
	    assert(encodedData.length==3);
		SecureRandom random = new SecureRandom();
		byte hashBytes[] = new byte[24]; //triple long eg 128+64 bits
		byte encodedBytes[] = null;
		do{
			random.nextBytes(hashBytes); 
			//update encodedBytes at the right positions to hold the encoded data		
			int byteIndex = 0;
			for(int i=0; i<encodedData.length; i++) {
				//System.out.println("record data: "+encodedData[i]);
				
				for(int b=0; b<8; b++) {
				    int mask = (1<<(byteIndex & 0x7));					
				    int value    = 0xFF & (hashBytes[byteIndex] & (~mask));	
				    int eDataBit = (encodedData[i]&mask);					
				    
				    int result = 0xFF&(value | eDataBit);
					
					hashBytes[byteIndex++] = (byte)result;
				}
			}		  			
			encodedBytes = Base64.getEncoder().encode(hashBytes); ///we encode AFTER we modify the bits.

			//after is valid filter we have
		} while (LicenseUtil.isInvalid(encodedBytes, invalidChars));

	    StringBuilder key = new StringBuilder();
	    key.append(new String(encodedBytes, 0,8));
	    key.append('-');
	    key.append(new String(encodedBytes, 8,8));
	    key.append('-');
	    key.append(new String(encodedBytes,16,8));
	    key.append('-');
	    key.append(new String(encodedBytes,24,8));
	    return key.toString();

	}

	static boolean isInvalid(byte[] encodedBytes, byte[] invalidChars) {
		//do not choose any with these chars as part of the encoding
		//do not reveal this list
		//we use no chars which may get confused
		//we use no vowels so no words can be spelled
		//we use no symbols for easier reading
		int i = encodedBytes.length;
		while (--i>=0) {
			int j = invalidChars.length;
			while (--j>=0) {
				if (invalidChars[j]==encodedBytes[i]) {
					return true;
				}
			}
		}
		return false;
	}


}
