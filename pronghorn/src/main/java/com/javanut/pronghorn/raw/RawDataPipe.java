package com.javanut.pronghorn.raw;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;

public class RawDataPipe extends Pipe<RawDataSchema> {

	private static class Schema extends RawDataSchema {
		public static final Schema instance = new Schema();
	}	
	
	public RawDataPipe(int items, int itemSize) {
		super(Schema.instance.newPipeConfig(items, itemSize));
	}
	
	public RawDataPipe(int items, int itemSize, RawDataSchema instance) {
		super(instance.newPipeConfig(items, itemSize));
	}
	
	
}
