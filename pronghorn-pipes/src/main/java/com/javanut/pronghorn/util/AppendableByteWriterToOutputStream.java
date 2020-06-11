package com.javanut.pronghorn.util;

import java.io.IOException;
import java.io.OutputStream;

public final class AppendableByteWriterToOutputStream implements AppendableByteWriter<AppendableByteWriterToOutputStream> {
	
	private final OutputStream writer;
	private final CharSequenceToUTF8 toUTF8 = new CharSequenceToUTF8();
	
	public AppendableByteWriterToOutputStream(OutputStream writer) {		
		this.writer = writer;
	}

	@Override
	public void write(byte[] backing) {
		try {
			writer.write(backing, 0, backing.length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void write(byte[] backing, int offset, int length) {
		try {
			writer.write(backing, 0, backing.length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeByte(int b) {
		try {
			writer.write(b);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendableByteWriterToOutputStream  append(CharSequence cs) {
		toUTF8.convert(cs).toOutputStream(writer);
		return this;
	}

	@Override
	public AppendableByteWriterToOutputStream append(char c) {
		toUTF8.convert(c).toOutputStream(writer);
		return null;
	}

	@Override
	public AppendableByteWriterToOutputStream append(CharSequence cs, int pos, int len) {
		toUTF8.convert(cs,pos,len).toOutputStream(writer);
		return null;
	}
}