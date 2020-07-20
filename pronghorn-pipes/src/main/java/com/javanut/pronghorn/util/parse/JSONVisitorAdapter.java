package com.javanut.pronghorn.util.parse;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.util.hash.IntHashTable;
import com.javanut.pronghorn.util.ByteConsumer;

//TODO: must move to pronghorn 0.0.19
public class JSONVisitorAdapter implements JSONVisitor {

	private int nameLen = 0;
	private byte[] nameBuffer = new byte[128]; 

	private int namePos = 0;	
	private int textPos = 0;	
	private byte[] textBuffer = new byte[128];
			
	ByteConsumer name = new ByteConsumer() {
		@Override
		public void consume(byte b) {
			
			if (namePos<nameBuffer.length) {
				nameBuffer[namePos++] = b;
			} else {
				byte[] newBuffer = new byte[nameBuffer.length*2];
				System.arraycopy(nameBuffer, 0, newBuffer, 0, nameBuffer.length);
				nameBuffer = newBuffer;
			    nameBuffer[namePos++] = b;
			}
		}

		@Override
		public void consume(byte[] buffer, int offset, int length, int m) {
			if (namePos+length<=buffer.length) {
				Pipe.copyBytesFromToRing(buffer, offset, m, 
					nameBuffer, namePos, Integer.MAX_VALUE, length);
			} else {
				byte[] newBuffer = new byte[1<<IntHashTable.computeBits(namePos+length)];
				System.arraycopy(nameBuffer, 0, newBuffer, 0, nameBuffer.length);
				nameBuffer = newBuffer;
				Pipe.copyBytesFromToRing(buffer, offset, m, 
					nameBuffer, namePos, Integer.MAX_VALUE, length);	
			}
			namePos += length;
		}			
	};
	
	
	ByteConsumer text = new ByteConsumer() {
		@Override
		public void consume(byte b) {
			if (textPos<textBuffer.length) {
				textBuffer[textPos++] = b;
			} else {
				byte[] newBuffer = new byte[textBuffer.length*2];
				System.arraycopy(textBuffer, 0, newBuffer, 0, textBuffer.length);
				textBuffer = newBuffer;
			    textBuffer[textPos++] = b;
			}
		}

		@Override
		public void consume(byte[] buffer, int offset, int length, int m) {
		    if (textPos+length<=textBuffer.length) {
				Pipe.copyBytesFromToRing(buffer, offset, m, 
					textBuffer, textPos, Integer.MAX_VALUE, length);
			} else {
				byte[] newBuffer = new byte[1<<IntHashTable.computeBits(textPos+length)];//TODO: find length??
				System.arraycopy(textBuffer, 0, newBuffer, 0, textBuffer.length);
				textBuffer = newBuffer;
			    Pipe.copyBytesFromToRing(buffer, offset, m, 
					 textBuffer, textPos, Integer.MAX_VALUE, length);
			}
			textPos += length;
		}			
	};


		protected final boolean nameMatches(byte[] key) {
			if (nameLen>=key.length) {
				int i = key.length;
				while (--i>=0) {
					if (key[i]!=nameBuffer[i]) {
						return false;
					}
				}
				return true;
			}
			return false;
		}
	
	@Override
		public final ByteConsumer stringName(int arg0) {
			return name;
		}

		@Override
		public final void stringNameComplete() {
			nameLen = namePos;
			namePos = 0;
		}

		@Override
		public final ByteConsumer stringValue() {		
			return text;
		}

		@Override
		public final void stringValueComplete() {
			stringValue(textBuffer, textPos);
			textPos = 0;
		}

	    protected void stringValue(byte[] data, int len) {
	    }

		@Override
		public void arrayBegin() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void arrayEnd() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void arrayIndexBegin(int arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void booleanValue(boolean arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void nullValue() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void numberValue(long arg0, byte arg1) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void objectBegin() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void objectEnd() {
			// TODO Auto-generated method stub
			
		}

}
