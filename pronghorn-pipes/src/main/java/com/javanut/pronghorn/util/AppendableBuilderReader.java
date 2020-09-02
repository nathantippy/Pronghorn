package com.javanut.pronghorn.util;

import java.io.IOException;
import java.io.OutputStream;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;

public class AppendableBuilderReader {
	
    byte[] buffer;	
	int byteCount;
	
	public AppendableBuilderReader() {
	}

	public void setData(AppendableBuilderReader source) {
		this.buffer = source.buffer;
		this.byteCount = source.byteCount;
	}
	
	void setData(byte[] buffer, int byteCount) {
			
		this.buffer = buffer;
		this.byteCount = byteCount;
		
	}
	
	public String toString() {
		return new String(buffer,0,byteCount);
	}
	
	public int byteLength() {
		return byteCount;
	}
	
	public void clear() {
		byteCount = 0;
	}
	
	public void parseSetup(TrieParserReader reader) {
		
		TrieParserReader.parseSetup(reader, buffer, 0, byteCount, buffer.length-1);
	}
	
	public void addToTrieParser(TrieParser parser, long value) {
		parser.setValue(buffer, 0, byteCount, Integer.MAX_VALUE, value);
	}
	
	
	public boolean startsWith(byte[] prefix) {
		return startsWith(prefix,prefix.length);
	}
	
	//starts with only the first len bytes of prefix
	public boolean startsWith(byte[] prefix, int len) {		
		int x = len;
		if (x <= byteCount) {
			while (--x >= 0) {
				if (prefix[x] != buffer[x]) {
					return false;
				}
			}
			return true;
		}		
		return false;
	}
	
	public boolean isEqual(byte[] data) {
		int x = data.length;
		if (x == byteCount) {
			while (--x >= 0) {
				if (data[x] != buffer[x]) {
					return false;
				}
			}
			return true;
		}		
		return false;
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

	public void copyTo(Appendable target) {
		
		if (target instanceof DataOutputBlobWriter) {
			((DataOutputBlobWriter)target).write(buffer, 0, byteCount, Integer.MAX_VALUE);
		} else {		
			Appendables.appendUTF8(target, buffer, 0, byteCount, Integer.MAX_VALUE);
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
