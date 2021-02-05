package com.javanut.pronghorn.raw;

import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;

public class RawDataWriter {
	public static final int MSG_SIZE = Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	public final RawDataPipe pipe;
	
	public RawDataWriter(RawDataPipe pipe) {
		this.pipe = pipe;
	}
		
	//////////////////////////
	// this are all written for easy use and so they can be "inlined" if needed
	//////////////////////////
	public final boolean hasRoomToWrite() {
		return Pipe.hasRoomForWrite(pipe, MSG_SIZE);
	}
	
	public final boolean hasRoomToWrite(int messages) {
		return Pipe.hasRoomForWrite(pipe, messages * MSG_SIZE);
	}

	public final void beginWriteMessage() {
		Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	}

	public final ChannelWriter openChannelWriter() {
		return Pipe.openOutputStream(pipe);
	}

	public final int endWriteMessage() {
		int len = Pipe.outputStream(pipe).closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, MSG_SIZE);
		Pipe.publishWrites(pipe);
		return len;
	}
	
	public final void publishShutdownMessage() {
		Pipe.publishEOF(pipe);;
	}
	
	  ///helper utility for arrays
	
		public static RawDataWriter[] wrap(RawDataPipe[] output) {
			RawDataWriter[] result = new RawDataWriter[output.length];
			int x = output.length;
			while (--x>=0) {
				result[x] = new RawDataWriter(output[x]);
			}
			return result;
		}
}
