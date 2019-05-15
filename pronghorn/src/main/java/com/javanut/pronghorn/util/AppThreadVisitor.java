package com.javanut.pronghorn.util;

public interface AppThreadVisitor {
	void visit(long threadId, long threadTId, long threadNId);
}
