package com.javanut.json.encode.function;

//@FunctionalInterface
public interface IterMemberFunction<T, M> {
    M get(T o, int i);
}

