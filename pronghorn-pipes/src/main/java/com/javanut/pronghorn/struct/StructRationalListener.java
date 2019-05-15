package com.javanut.pronghorn.struct;

public interface StructRationalListener {

	void value(long numerator, long denominator, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
