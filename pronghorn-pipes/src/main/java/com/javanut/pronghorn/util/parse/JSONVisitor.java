package com.javanut.pronghorn.util.parse;

import com.javanut.pronghorn.util.ByteConsumer;

public interface JSONVisitor {

	ByteConsumer stringValue();

	void stringValueComplete();

	ByteConsumer stringName(int fieldIdx);

	void stringNameComplete();

	void arrayBegin();

	void arrayEnd();

	void arrayIndexBegin(int instance);

	void numberValue(long m, byte e);

	void nullValue();

	void booleanValue(boolean b);

	void objectEnd();

	void objectBegin();

	
}
