package com.javanut.pronghorn.util.template;

import com.javanut.pronghorn.util.AppendableByteWriter;

public abstract class StringTemplateRenderer<T> {

	public abstract void render(AppendableByteWriter<?> writer, T source);
	
}
