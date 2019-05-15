package com.javanut.pronghorn.util;

public interface TrieParserVisitor {

	void visit(byte[] backing, int length, long value);
	
}
