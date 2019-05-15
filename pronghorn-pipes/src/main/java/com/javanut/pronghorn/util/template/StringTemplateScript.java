package com.javanut.pronghorn.util.template;

import com.javanut.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public abstract class StringTemplateScript<T> {
	public abstract void render(AppendableByteWriter appendable, T source);
}
