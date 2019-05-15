package com.javanut.json.encode.function;

import com.javanut.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface ToStringFunction<T> {
	void applyAsString(T value, AppendableByteWriter<?> target);
}
