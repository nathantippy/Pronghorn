package com.javanut.json.encode.function;

import com.javanut.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface IterStringFunction<T> {
    void applyAsString(T o, int i, AppendableByteWriter<?> target);
}
