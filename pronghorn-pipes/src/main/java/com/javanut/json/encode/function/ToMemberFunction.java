package com.javanut.json.encode.function;

//@FunctionalInterface
public interface ToMemberFunction<T, M> {
    M get(T o);
}

