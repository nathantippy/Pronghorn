package com.javanut.pronghorn.util;

public interface TrieKeyable<T extends Enum<T>& TrieKeyable> {

	public CharSequence getKey();
	
	
	
}
