package com.javanut.pronghorn.struct;

import com.javanut.pronghorn.pipe.ChannelReader;

public interface StructBlobListener {

	void value(ChannelReader reader, int[] position, int[] size, int instance, int totalCount);
	
}
