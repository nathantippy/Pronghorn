package com.javanut.pronghorn.struct;

import com.javanut.pronghorn.pipe.ChannelReader;

public interface StructFieldVisitor<T> {
	
	public void read(T value, ChannelReader reader, long fieldId);
	
}
