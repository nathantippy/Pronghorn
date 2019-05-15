package com.javanut.pronghorn.util;

public abstract class ServerObjectHolderVisitor<T> {

	public abstract void visit(int idx, T t);

}
