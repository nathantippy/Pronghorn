package com.javanut.pronghorn.struct;

import com.javanut.pronghorn.pipe.TextReader;

public interface StructTextListener {

	void value(TextReader reader, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
