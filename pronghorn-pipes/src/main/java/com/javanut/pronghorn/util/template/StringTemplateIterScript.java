package com.javanut.pronghorn.util.template;

import com.javanut.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface StringTemplateIterScript<T> {
	boolean render(AppendableByteWriter<?> appendable, T source, int i);
}

