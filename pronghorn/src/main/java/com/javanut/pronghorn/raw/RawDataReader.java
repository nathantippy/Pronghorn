package com.javanut.pronghorn.raw;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;

public class RawDataReader {
	public static final int MSG_SIZE = Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	public final RawDataPipe pipe;
	
	
	public RawDataReader(RawDataPipe pipe) {
		this.pipe = pipe;
	}


	//////////////////////////
	// this are all written for easy use and so they can be "inlined" if needed
	//////////////////////////

	private boolean normal;
	
	public final boolean hasMessageToRead() {
		return Pipe.hasContentToRead(pipe);
	}

	public final boolean beginReadMessage() {
		return normal = (Pipe.takeMsgIdx(pipe) >= 0);
	}

	public final ChannelReader openChannelReader() {
		return Pipe.openInputStream(pipe);
	}

	public final void endReadMessage() {
		Pipe.confirmLowLevelRead(pipe, normal ? MSG_SIZE : Pipe.EOF_SIZE);
		Pipe.releaseReadLock(pipe);
	}

    ///helper utility for arrays
	
	public static RawDataReader[] wrap(RawDataPipe[] input) {
		RawDataReader[] result = new RawDataReader[input.length];
		int x = input.length;
		while (--x>=0) {
			result[x] = new RawDataReader(input[x]);
		}
		return result;
	}
	
}
