package com.javanut.pronghorn.util;

import com.javanut.pronghorn.pipe.StructuredReader;

@FunctionalInterface
public interface JSON2StructVisitor {

	public void visit(StructuredReader reader);
}
