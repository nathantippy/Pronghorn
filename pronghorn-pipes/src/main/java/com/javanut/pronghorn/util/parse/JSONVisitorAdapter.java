package com.javanut.pronghorn.util.parse;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.util.hash.IntHashTable;
import com.javanut.pronghorn.util.Appendables;
import com.javanut.pronghorn.util.ByteConsumer;
import com.javanut.pronghorn.util.TrieParser;
import com.javanut.pronghorn.util.TrieParserReader;

public class JSONVisitorAdapter implements JSONVisitor {

	private int nameLen = 0;
	private byte[] nameBuffer = new byte[128]; 

	private int namePos = 0;	
	private int textPos = 0;	
	private byte[] textBuffer = new byte[128];
		
	private byte[] bytePath = new byte[128]; //TODO: when can we reset this for re-use?
	private int bytePathLength = 0;

	private int[] arrayIndexStack = new int[16];
	private int arrayIndexStackPos = -1;
	
	public void clear() {
		nameLen = 0;
		namePos = 0;
		textPos = 0;
		bytePathLength = 0;		
	}
	
	
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
		
		protected String name() {
			return new String(nameBuffer, 0, nameLen );
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
			int temp = bytePathLength;
			if (nameLen>0) {
				System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
				bytePathLength+=nameLen;
			}
			
			stringData(textBuffer, textPos);
						
			bytePathLength = temp;
			textPos = 0;
		}

        /////////////////////////////////////////
		//build stack based position
		//human readable and GC free
		/////////////////////////////////////////

		
		protected final long pathMatches(TrieParser parser, TrieParserReader reader) {
			TrieParserReader.parseSetup(reader, bytePath, 0, bytePathLength, bytePath.length-1);
			long result = reader.parseNext(parser);
			return !TrieParserReader.parseHasContent(reader) ? result : -1;
		}
		protected final String path() {
			return new String(bytePath, 0, bytePathLength);		
		}		
		
		@Override
		public final void objectBegin() {     //          [{thing:ting}, ]
			//parent may be array entry 
			int req = Math.abs(bytePathLength);

			if ((bytePathLength+1+req)<bytePath.length) {
				//append
				if (nameLen>0) {
					System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
					bytePathLength+=nameLen;
					nameLen = -1;//ensure its not used a second time.
				}
				bytePath[bytePathLength++]='.';			
			} else {
				//grow
				growPath(bytePathLength+1+req);				
			    //append
				if (nameLen>0) {
					System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
					bytePathLength+=nameLen;
					nameLen = -1;//ensure its not used a second time.
				}
				bytePath[bytePathLength++]='.';
			}			
		}

		private final void growPath(int desiredLen) {
			int bits = (int)Math.ceil(Math.log(desiredLen)/Math.log(2));
			byte[] newArray = new byte[1<<bits];
			System.arraycopy(bytePath, 0, newArray, 0, bytePathLength);
			bytePath = newArray;
		}

		@Override
		public final void objectEnd() {
			rollBackPathTo((byte) '.');
			if (bytePath[bytePathLength-1]==']') {
				rollBackPathTo((byte) '[');
				bytePathLength--;
			} else {
				bytePathLength--;
				rollBackPathName();
			}
			nameLen = -1;//ensure last name is not used.
		} 
		
		protected int arrayDepth() {
			return arrayIndexStackPos+1;
		}
		protected int arrayIndex() { //just the top
			return arrayIndexStack[arrayIndexStackPos];
		}
		
		
		@Override
		public final void arrayBegin() {
			if (++arrayIndexStackPos >= arrayIndexStack.length) {
				int[] newArray = new int[arrayIndexStack.length*2];
				System.arraycopy(arrayIndexStack, 0, newArray, 0, arrayIndexStack.length);
				arrayIndexStack = newArray;				
			}			
			
			int req = Math.abs(bytePathLength);
			
			if ((bytePathLength+1+req)<bytePath.length) {
				//append
				if (nameLen>0) {
					System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
					bytePathLength+=nameLen;
					nameLen = -1;//ensure its not used a second time.
				}
				bytePath[bytePathLength++]='[';			
			} else {
				//grow
				growPath(bytePathLength+1+req);				
			    //append
				if (nameLen>0) {
					System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
					bytePathLength+=nameLen;
					nameLen = -1;//ensure its not used a second time.
				}
				bytePath[bytePathLength++]='[';
			}
			beginArray();
		}

		@Override
		public final void arrayEnd() {
			rollBackPathTo((byte) '[');
			if (bytePath[bytePathLength-1]==']') {
				rollBackPathTo((byte) '[');
				bytePathLength--;
			} else {
				bytePathLength--;
				rollBackPathName();				
			}
			endArray();
			arrayIndexStackPos--;
		}

		@Override
		public final void arrayIndexBegin(int value) {
			
			
			arrayIndexStack[arrayIndexStackPos] = value;;
			
			//delete everything until we find the [ stop and keep the [
			nameLen = -1;//ensure last name is not used.
			rollBackPathTo((byte) '[');
			
			// we know there is enough room for the largest int + the ] char
			
			if ((bytePathLength+1+10)<bytePath.length) {
				//append
				bytePathLength = Appendables.longToChars(value, false, bytePath, Integer.MAX_VALUE, bytePathLength);
				bytePath[bytePathLength++]=']';			
			} else {
				//grow
				growPath(bytePathLength+1+10);				
			    //append		
				bytePath[bytePathLength++]=']';
			}		
			startArrayItem(value);
		}

		private final void rollBackPathTo(byte target) {
			int x = bytePathLength;
			while (--x>=0) {
				if (bytePath[x] == target) {
					bytePathLength = x+1;
					break;
				}
			}
		}
		private final void rollBackPathName() {
			int x = bytePathLength;
			while (--x>=0) {
				if (bytePath[x] == ']' || bytePath[x] =='.') {
					bytePathLength = x+1;
					return;
				}
			}
			bytePathLength = 0;//nothing found in front
		}
		
		////////////////////////////////////
		@Override
		public final void booleanValue(boolean value) {
			int temp = bytePathLength;
			if (nameLen>0) {
				System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
				bytePathLength+=nameLen;
			}
			booleanData(value);
			bytePathLength = temp;
		}
		
		@Override
		public final void nullValue() {
			int temp = bytePathLength;
			if (nameLen>0) {
				System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
				bytePathLength+=nameLen;
			}
			nullData();
			bytePathLength = temp;
		}
		
		@Override
		public final void numberValue(long m, byte e) {
			int temp = bytePathLength;
			if (nameLen>0) {
				System.arraycopy(nameBuffer, 0, bytePath, bytePathLength, nameLen);
				bytePathLength+=nameLen;
			}
			numberData(m,e);
			bytePathLength = temp;
		}

		protected void beginArray() {
		}
		protected void endArray() {
		}
		protected void startArrayItem(int idx) {
		}
		
	    protected void stringData(byte[] data, int len) {
	    }
	    
	    protected void booleanData(boolean value) {
	    }
	    
	    protected void nullData() {
	    }
	    
	    protected void numberData(long m, byte e) {
	    }



}
