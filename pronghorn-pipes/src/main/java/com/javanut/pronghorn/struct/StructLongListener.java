package com.javanut.pronghorn.struct;

public interface StructLongListener {

	void value(long value, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
