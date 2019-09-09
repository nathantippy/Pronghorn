package com.javanut.pronghorn.pipe;

import sun.misc.Unsafe;

import java.lang.annotation.Native;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeUtil {

	///////
	//https://highlyscalable.wordpress.com/2012/02/02/direct-memory-access-in-java/
	///////
	
	
	public static Unsafe getUnsafe() {
	    try {
	            Field f = Unsafe.class.getDeclaredField("theUnsafe");
	            f.setAccessible(true);
	            return (Unsafe)f.get(null);
	    } catch (Exception e) { /* ... */ }
	    return null;
	}
	
	public static long sizeOf(Unsafe unsafe, Object object) {
				
		   return unsafe.getAddress( normalize( unsafe.getInt(object, 4L) ) + 12L );
		}
		 
	public static long normalize(int value) {
	   if(value >= 0) return value;
	   return (~0L >>> 32) & value;
	}
	
	
	public static long getAddressOfObject(sun.misc.Unsafe unsafe, Object obj) {
	    Object helperArray[]    = new Object[1];
	    helperArray[0]          = obj;
	    long baseOffset         = unsafe.arrayBaseOffset(Object[].class);
	    long addressOfObject    = unsafe.getLong(helperArray, baseOffset);      
	    return addressOfObject;
	}
	
	
}
