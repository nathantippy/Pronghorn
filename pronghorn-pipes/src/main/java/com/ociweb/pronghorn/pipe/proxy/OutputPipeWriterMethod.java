package com.ociweb.pronghorn.pipe.proxy;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public abstract class OutputPipeWriterMethod {
	
	public abstract void write(Object[] args);
	
	static final char[] EMPTY_CHAR = new char[0];
	static final byte[] EMPTY_BYTES = new byte[0];
	
	static OutputPipeWriterMethod buildWriteForYourType(final Pipe pipe, final int decimalPlaces, final int fieldLoc, final int extractedType, final FieldReferenceOffsetManager from) {
		final int absent32 = FieldReferenceOffsetManager.getAbsent32Value(from);
		final long absent64 = FieldReferenceOffsetManager.getAbsent64Value(from);
	
		//NOTE: the code in these anonymous classes is the same code that must be injected when the compile time code generation is done.
		
		switch (extractedType) {
			case 0:
			case 2:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						PipeWriter.writeInt(pipe, fieldLoc, ((Number)args[0]).intValue());
					}
				};
			case 1:
			case 3:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null == args[0]) {
							PipeWriter.writeInt(pipe, fieldLoc, absent32);
						} else {
							PipeWriter.writeInt(pipe, fieldLoc, ((Number)args[0]).intValue());	
						}
					}
				};
			case 4:
			case 6:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						PipeWriter.writeLong(pipe, fieldLoc, ((Number)args[0]).longValue());
					}
				};
			case 5:
			case 7:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null == args[0]) {
							PipeWriter.writeLong(pipe, fieldLoc, absent64);
						} else {
							PipeWriter.writeLong(pipe, fieldLoc, ((Number)args[0]).longValue());	
						}
					}
				};
			case 8:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						PipeWriter.writeASCII(pipe, fieldLoc, args[0].toString());
					}
				};
			case 9:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							PipeWriter.writeASCII(pipe, fieldLoc, EMPTY_CHAR, 0, -1);
						} else {
							PipeWriter.writeASCII(pipe, fieldLoc, args[0].toString());
						}
					}
				};
			case 10:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						PipeWriter.writeUTF8(pipe, fieldLoc, args[0].toString());
					}
				};
			case 11:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							PipeWriter.writeUTF8(pipe, fieldLoc, EMPTY_CHAR, 0, -1);
						} else {
							PipeWriter.writeUTF8(pipe, fieldLoc, args[0].toString());
						}
					}
				};
			case 12:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						PipeWriter.writeDouble(pipe, fieldLoc, ((Number)args[0]).doubleValue(), decimalPlaces);
					}
				};
			case 13:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							PipeWriter.writeDecimal(pipe, fieldLoc, absent32, absent64);
						} else {
							PipeWriter.writeDouble(pipe, fieldLoc, ((Number)args[0]).doubleValue(), decimalPlaces);	
						}
					}
				};
			case 14:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (args[0] instanceof ByteBuffer) {
							PipeWriter.writeBytes(pipe, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
						} else {
							PipeWriter.writeBytes(pipe, fieldLoc, (byte[])args[0]);					
						}
					}
				};
			case 15:
				return new OutputPipeWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							PipeWriter.writeBytes(pipe, fieldLoc, EMPTY_BYTES, 0, -1, 1);
						} else {
							if (args[0] instanceof ByteBuffer) {//NOTE: investigate returning wraper of backing array.
								PipeWriter.writeBytes(pipe, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
							} else {
								PipeWriter.writeBytes(pipe, fieldLoc, (byte[])args[0]);					
							}
						}
					}
				};
			default:
				throw new UnsupportedOperationException("No support yet for "
						+ TypeMask.xmlTypeName[extractedType]);
	
		}
	}

}
