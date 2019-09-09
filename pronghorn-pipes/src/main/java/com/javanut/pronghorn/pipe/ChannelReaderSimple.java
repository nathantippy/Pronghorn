package com.javanut.pronghorn.pipe;

import java.io.InputStream;

//minimum reader to be implemented by direct buffers etc
public abstract class ChannelReaderSimple extends InputStream {

	
	/**
	 * Open low level field at this point in the pipe.
	 * @return available length in bytes
	 */
	public abstract int openLowLevelAPIField();
	
	/**
	 * Will give how many bytes are available right now
	 * @return the number of bytes
	 */
	public abstract int available();
	
	/**
	 * Read len bytes into target b array at location off wraping on mask
	 * @param b target array to write into
	 * @param off position where to begin writing
	 * @param len total count of bytes to copy
	 * @param mask wrap at this value
	 * @return bytes written count
	 */
	public abstract int readInto(byte[] b, int off, int len, int mask);
	
}
