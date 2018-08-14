package com.ociweb.pronghorn.util.parse;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.util.ByteConsumer;
import com.ociweb.pronghorn.util.TrieKeyable;
import com.ociweb.pronghorn.util.math.Decimal;

public class JSONStreamVisitorToPipe<M extends MessageSchema<M>, K extends Enum<K> & TrieKeyable> implements JSONStreamVisitor {

	//TODO: this would be much faster if it were code generated, but we must ensure the dynamic behavior is right first.
	
	
	//private final FieldReferenceOffsetManager from;
	private final Pipe<M> pipe;
	
	//map array sequence check against field in a message then apply
	
	///////////////
	//these stacks are helpful for debugging
	private final long[] uStack;//these are the unique positions, (prev*len)+pos path
	private final K[] enumStack;
	private final K[] keys;
	
	private final int BottomOfJSON;

	private int stackPosition=0;
	
	private final MapJSONToPipeBuilder<M, K> mapper;
	private boolean isRecordOpen = false;

	private int activeFieldLoc = 0;
	private int activeFieldBit = 0;
	
	//for "no mach" we will use this object to consume the data
	private final ByteConsumer nullByteConsumer = new ByteConsumer() {
		@Override
		public void consume(byte[] backing, int pos, int len, int mask) {	
		}
		@Override
		public void consume(byte value) {
		}
	};
	
	
	public JSONStreamVisitorToPipe(Pipe<M> pipe, Class<K> keys, int bottom, MapJSONToPipeBuilder<M, K> mapper) {
		
		//this.from = null==pipe? null :Pipe.from(pipe);
		this.pipe = pipe;		
		this.keys = keys.getEnumConstants();
		
		final int maxStack = 32;//9;  //TOOD: use long of keys.length to compute this value.
		
		this.enumStack = (K[]) new Enum[maxStack];
		this.uStack = new long[maxStack];
		this.mapper = mapper;
		this.BottomOfJSON = bottom;
	
	}

	@Override
	public boolean isReady() {
		return PipeWriter.hasRoomForWrite(pipe);
	}
	
	
	@Override
	public void nameSeparator() {
	}

	@Override
	public void endObject() {
		
		uStack[stackPosition] = 0;
		enumStack[stackPosition] = null;
		
		stackPosition--;
		
		//System.err.println("parse level: "+stackPosition);
		
		//write out the captured record
		if (stackPosition<=BottomOfJSON && isRecordOpen) {
			//System.err.println("new message closed");			
			PipeWriter.publishWrites(pipe);
			
			isRecordOpen = false;
		}
		
		
	}

	@Override
	public void beginObject() {
		stackPosition++;
	}

	@Override
	public void beginArray() {	
		
		enumStack[stackPosition] = null;
				
		stackPosition++;

	}

	@Override
	public void endArray() {
		
		uStack[stackPosition] = 0;
		enumStack[stackPosition] = null;
		
		stackPosition--;
		
	}

	@Override
	public void valueSeparator() {		
		//no array support at this time.	
	}

	@Override
	public void whiteSpace(byte b) {		
	}

	@Override
	public void literalTrue() {
		
		if (activeFieldLoc>0) {
			if (mapper.isInteger(activeFieldLoc)) {
							
				if (0 != activeFieldBit) {					
					PipeWriter.accumulateBitsValue(pipe, activeFieldLoc, activeFieldBit);
				} else {
					writeInt(activeFieldLoc, 1);
				}
			} else {
				writeUTF8(activeFieldLoc, "true");
			}
			activeFieldLoc = 0;
		}
		
	}

	@Override
	public void literalNull() {
		
		if (activeFieldLoc>0) {
			if (mapper.isInteger(activeFieldLoc)) {
				writeInt(activeFieldLoc, -1);
			} else {
				if (mapper.isLong(activeFieldLoc)) {
					writeLong(activeFieldLoc, -1);
				} else {
				    writeUTF8(activeFieldLoc, "null");
				}
			}
			activeFieldLoc = 0;
		}		
	}

	@Override
	public void literalFalse() {
		
		if (activeFieldLoc>0) {
			if (mapper.isInteger(activeFieldLoc)) {
				writeInt(activeFieldLoc, 0);
			} else {
				writeUTF8(activeFieldLoc, "false");
			}
			activeFieldLoc = 0;
		}
		
	}

	protected void writeUTF8(int loc, CharSequence value) {
		PipeWriter.writeUTF8(pipe, loc, value);
	}
	
	protected void writeInt(int loc, int value) {
		PipeWriter.writeInt(pipe, loc, value);
	}
	
	protected void writeLong(int loc, long value) {
		PipeWriter.writeLong(pipe, loc, value);
	}
	
	protected void writeDecimal(int loc, byte e, long m) {
		PipeWriter.writeDecimal(pipe, loc, e, m);
	}
	
	
	@Override
	public void numberValue(long m, byte e) {
		
		
		if (activeFieldLoc>0) {
			if (mapper.isInteger(activeFieldLoc)) {
				writeInt(activeFieldLoc, (int) Decimal.asLong(m, e));				
			} else if (mapper.isLong(activeFieldLoc)) {
				writeLong(activeFieldLoc, (long) Decimal.asLong(m, e));
			} else {
				writeDecimal(activeFieldLoc, e, m);
				
			}
			activeFieldLoc = 0;
		}
	}

	@Override
	public void stringBegin() {
		
		if (activeFieldLoc>0) {
					
				DataOutputBlobWriter<M> out = PipeWriter.outputStream(pipe);
				DataOutputBlobWriter.openField(out);
		
		}
		
	}

	@Override
	public ByteConsumer stringAccumulator() { 

		if (activeFieldLoc>0) {
			
			return PipeWriter.outputStream(pipe);
		} else {
			return nullByteConsumer;
		}
	}

	@Override
	public void stringEnd() {
		if (activeFieldLoc>0) {
			
			if (!mapper.isNumeric(activeFieldLoc)) {
			
				
				DataOutputBlobWriter<M> out = PipeWriter.outputStream(pipe);

				//this data looks correct.
				//byte[] byteArray = out.toByteArray();
				//System.err.println(activeFieldLoc+" CAPTURED TEXT:"+new String(byteArray));
								
				
				int len = DataOutputBlobWriter.closeHighLevelField(out, activeFieldLoc);
				
				
				
			} else {
				pipe.closeBlobFieldWrite();			
			    				
				//	System.err.println("UNABLE TO PARSE NUMERIC:"+new String(out.toByteArray()));
				
			}
			
			activeFieldLoc = 0;
		}
	}

	@Override
	public void customString(int id) {
		assert(id>=0);
		
		//no support at this time for values found at the root
		if (stackPosition<=0) {
			return;
		}
		
		enumStack[stackPosition-1] = keys[id];
		
		long value;
		if (stackPosition>1) {
			value = (uStack[stackPosition-2]*keys.length)+id;
		} else {
			value = id;
		}
		
		uStack[stackPosition-1] = value;	
		activeFieldLoc = mapper.getLoc(value);
		activeFieldBit = mapper.bitMask(value);
		
		//System.err.println("open loc "+activeFieldLoc);
		
		//open new record
		if (activeFieldLoc>0 && !isRecordOpen) {
			
			
			//will not block because the caller will have already checked.
			boolean ok = PipeWriter.tryWriteFragment(pipe, mapper.messageId());
			assert(ok);
			
			///////////////////////
			//clear all the bit fields so we start with zero 
			////////////////////////
			int[] bitFields = mapper.bitFields();
			int i = bitFields.length;
			while (--i>=0) {
				PipeWriter.writeInt(pipe, bitFields[i], 0);
			}
			
			
			isRecordOpen = true;
		}
		
		
	}

}
