package com.javanut.json.encode.function;

//@FunctionalInterface
public interface IterEnumFunction<T, E extends Enum<E>> {
    E applyAsEnum(T value, int i);
}
