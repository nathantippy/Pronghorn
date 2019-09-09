package com.javanut.pronghorn.pipe;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ChannelReaderSimpleDirectBuffer extends ChannelReaderSimple {

	private final Pipe<?> pipe;
	private int length;
	private ByteBuffer[] buffers;
	
	public ChannelReaderSimpleDirectBuffer(Pipe<?> pipe) {
		this.pipe = pipe;
	}

	@Override
	public int openLowLevelAPIField() {
		
		    final int meta = Pipe.takeByteArrayMetaData(this.pipe);
	        	                
			this.length    = Math.max(0, Pipe.takeByteArrayLength(this.pipe));			
			this.buffers = Pipe.wrappedReadingDirectBuffers(pipe, meta, this.length);					 		
			assert(this.buffers[0].isDirect());
			
			return this.length;
	}

	@Override
	public int available() {
		return buffers[0].remaining()+buffers[1].remaining();
	}
	
	@Override
	public int read() throws IOException {
		if (buffers[0].hasRemaining()) {
			return 0xFF&buffers[0].get();
		} else  if (buffers[1].hasRemaining()) {
			return 0xFF&buffers[1].get();
		} else {
			return -1;
		}
	}

	@Override
	public int readInto(byte[] b, int off, int len, int mask) {
		final int wittenCount = Math.min(len, buffers[0].remaining()+buffers[1].remaining());
		if (buffers[0].hasRemaining()) {
			//may need to write 1 or 2 blocks
			int localLen = Math.min(buffers[0].remaining(), len);			
			Pipe.copyBytesFromByteBufferToRing(buffers[0], b, off, mask, localLen);			
			len -= localLen;
			off += localLen;
		} 
		if (len>0 && buffers[1].hasRemaining()) {
			//may need to write 1 or 2 blocks
			int localLen = Math.min(buffers[1].remaining(), len);			
			Pipe.copyBytesFromByteBufferToRing(buffers[1], b, off, mask, localLen);			
			//len -= localLen;
		} 
		return wittenCount;
	}

}
