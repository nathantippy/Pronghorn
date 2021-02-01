package com.javanut.pronghorn.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;

public class AppendableBuilder implements AppendableByteWriter<AppendableBuilder> {

	private int maximumAllocation;
	private static final int defaultSize = 1<<14; //must be power of 2
	
    private byte[] buffer;	
	private int byteCount;
	
	private AppendableBuilderReader abir = null;

	public AppendableBuilder() {
		this(Integer.MAX_VALUE);
	}
	
	public AppendableBuilder(InputStream input) {
		this(Integer.MAX_VALUE);
		try {
			consumeAll(input);
			input.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	//This class is allowed to grow but only up to the maximumAllocation
	public AppendableBuilder(int maximumAllocation) {
		
		this.maximumAllocation = maximumAllocation;		
		this.buffer = new byte[maximumAllocation<defaultSize?maximumAllocation:defaultSize];

	}
	
	public void clear() {
		byteCount = 0;	
	}
	
	public AppendableBuilderReader reader() {
		if (null!=abir) {
			abir.setData(buffer, byteCount);
			return abir;
		} else {
			abir = new AppendableBuilderReader();
			abir.setData(buffer, byteCount);
			return abir;
		}
	}
	
	public void parseSetup(TrieParserReader reader) {
		TrieParserReader.parseSetup(reader, buffer, 0, byteCount, buffer.length-1);
	}

	public String toString() {
		return new String(buffer,0,byteCount);
	}
	
	public int byteLength() {
		return byteCount;
	}

	public int copyTo(ChannelWriter target, int sourcePos) {	
		int len = Math.min(target.remaining(), byteCount-sourcePos);		
		target.write(buffer,sourcePos,len);
		return len;
	}
	
	public int copyTo(OutputStream target) {	
		return copyTo(Integer.MAX_VALUE, target);
	}
	
	public int copyTo(int maxBytes, OutputStream target) {	
		int pos = 0;
		int len = Math.min(maxBytes, (byteCount-pos));
		assert(len>=0);
		try {
			target.write(buffer, pos, len);
			pos += len;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return len;
	}	

	public static void copy(AppendableBuilder source, AppendableBuilder target) {
		target.write(source.buffer, 0, source.byteCount);	
	}
	
	public static void copy(AppendableBuilderReader source, AppendableBuilder target) {
		target.write(source.buffer, 0, source.byteCount);	
	}
	
	public void copyTo(Appendable target) {
		
		if (target instanceof DataOutputBlobWriter) {
			((DataOutputBlobWriter)target).write(buffer, 0, byteCount, Integer.MAX_VALUE);
		} else {		
			Appendables.appendUTF8(target, buffer, 0, byteCount, Integer.MAX_VALUE);
		}
		
	}

	public static void appendLongAsText(AppendableBuilder ab, long value, boolean useNegPara) {
		
		if (ab.byteCount+(18) <= ab.buffer.length) {		
			ab.byteCount = Appendables.longToChars(value, useNegPara, ab.buffer, 
	        		                                        Integer.MAX_VALUE, ab.byteCount);
		} else {
			growNow(ab, ab.byteCount+(18));
			ab.byteCount = Appendables.longToChars(value, useNegPara, ab.buffer, 
                    Integer.MAX_VALUE, ab.byteCount);
		}
	}
	
	@Override
	public AppendableBuilder append(CharSequence csq) {
		
		int len = csq.length();		
		if (byteCount+(len<<3) <= buffer.length) {
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, 0, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;		
		} else {
			growNow(this,byteCount+(len<<3));			
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, 0, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;
		}

		return this;
	}

	
	private static void growNow(AppendableBuilder that, int req) {
		if (req > that.maximumAllocation) {
			throw new UnsupportedOperationException("Max allocation was limited to "+that.maximumAllocation+" but more space needed");
		}
		
		//bufer MUST be power of 2 due to TrieParse requrements elsewhere
		int bits = (int)Math.ceil(Math.log(req)/Math.log(2)); //find next power to hold this value
		byte[] temp = new byte[1<<bits];
		System.arraycopy(that.buffer, 0, temp, 0, that.buffer.length);
		that.buffer = temp;
	}

	@Override
	public AppendableBuilder append(CharSequence csq, int start, int end) {
		
		int len = end-start;

		if (byteCount+(len<<3) <= buffer.length) {
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, start, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;
		} else {
			growNow(this,byteCount+(len<<3));			
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, start, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;

		}

		return this;
	}

	@Override
	public AppendableBuilder append(char c) {	
		if (byteCount+(1<<3) <= buffer.length) {
			byteCount = Pipe.encodeSingleChar(c, buffer, Integer.MAX_VALUE, byteCount);
		} else {		
			growNow(this,byteCount+(1<<3));			
			byteCount = Pipe.encodeSingleChar(c, buffer, Integer.MAX_VALUE, byteCount);
		}
		return this;
	}



    public void consumeAll(InputStream input) throws IOException {

				int maxLen = buffer.length-byteCount;
				int curPos = byteCount;
				
				//System.out.println("length: "+maxLen);
				if (maxLen<8) {
					growNow(this,byteCount+1024);
					maxLen = buffer.length-byteCount;
				} 
				 
				int len = -1;
				while ( (len = input.read(buffer, curPos, maxLen)) >= 0 ) {
					
					//System.out.println("unknown length:"+len);
		
					curPos += len;					
					maxLen -= len;
				    byteCount += len;
				
				    //System.out.println("length: "+maxLen);	
					if (maxLen<8) {
					    growNow(this, byteCount+1024);
					    maxLen = buffer.length-byteCount;
					}
					
					
					
				}
			    
    }




	@Override
	public void write(byte[] encodedBlock, int pos, int len) {
		
		if (byteCount+len <= buffer.length) {
			System.arraycopy(encodedBlock, pos, buffer, byteCount, len);
		} else {
			growNow(this,byteCount+len);			
			System.arraycopy(encodedBlock, pos, buffer, byteCount, len);		
		}		
		this.byteCount+=len;
	}

	@Override
	public void write(byte[] encodedBlock) {
		
		if (byteCount+encodedBlock.length <= buffer.length) {
			System.arraycopy(encodedBlock, 0, buffer, byteCount, encodedBlock.length);		
			this.byteCount+=encodedBlock.length;
		} else {
			growNow(this,byteCount+encodedBlock.length);
			System.arraycopy(encodedBlock, 0, buffer, byteCount, encodedBlock.length);
			this.byteCount+=encodedBlock.length;
		}
	}

	@Override
	public void writeByte(int asciiChar) {
		
		if (byteCount+1 <= buffer.length) {
			buffer[byteCount++] = (byte)asciiChar;
		} else {
			growNow(this,byteCount+1);			
			buffer[byteCount++] = (byte)asciiChar;
		}
	}

	public int absolutePosition() {
		return byteCount;
	}

	public void absolutePosition(int absolutePosition) {
		byteCount = absolutePosition;
	}

	public boolean isEqual(ChannelReader target) {
		return target.equalBytes(buffer, 0, byteCount);
	}
	
	public int base64Decode(byte[] target, int targetIdx, int targetMask) {
		return Appendables.decodeBase64(buffer, 0, byteCount, Integer.MAX_VALUE,
							    target, targetIdx, targetMask);
	}

	public byte[] toBytes() {
		byte[] result = new byte[byteCount];
		System.arraycopy(this.buffer, 0, result, 0, result.length);
		return result;
	}


}
