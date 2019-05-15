package com.javanut.json.encode.function;

//@FunctionalInterface
public interface ToDoubleFunction<T> {
    double applyAsDouble(T value);
}
