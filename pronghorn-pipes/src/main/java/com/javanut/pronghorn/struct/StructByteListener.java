package com.javanut.pronghorn.struct;

public interface StructByteListener {
	
	void value(byte value, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
