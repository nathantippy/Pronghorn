package com.javanut.pronghorn.util;

import java.lang.reflect.Array;

public class ArrayGrow {


	public static boolean[] appendToArray(boolean[] array, boolean newItem) {
		
		int i = null==array?0:array.length;
		boolean[] newArray = new boolean[i+1];
		System.arraycopy(array, 0, newArray, 0, i);
		newArray[i] = newItem;
		return newArray;
	}
	
	public static int[] appendToArray(int[] array, int newItem) {
		
		int i = null==array?0:array.length;
		int[] newArray = new int[i+1];
		System.arraycopy(array, 0, newArray, 0, i);
		newArray[i] = newItem;
		return newArray;
	}

	public static <T> T[] appendToArray(T[] source, T append) {
		
		int i = null==source?0:source.length;
		T[] newArray = (T[]) Array.newInstance(append.getClass(), i+1);
		if (source!=null) {
			System.arraycopy(source, 0, newArray, 0, i);
		}
		newArray[i] = append;
		return newArray;
	}
	
	public static <T> T[] appendToArray(T[] source, T append, Class<?> clazz) {
		
		int i = null==source ? 0 : source.length;
		T[] newArray = (T[]) Array.newInstance(clazz, i + 1);
		System.arraycopy(source, 0, newArray, 0, i);
		newArray[i] = append;
		return newArray;
	}

	
	public static boolean[] setIntoArray(boolean[] source, boolean obj, int pos) {
		
		int i = source.length;
		if (pos>=i) {
			int newSize = i>0 ? i*2 : 2;
			if (pos>=newSize) {
				newSize = pos>0 ? pos*2 : 2;
			}
			boolean[] newArray = new boolean[newSize];
			System.arraycopy(source, 0, newArray, 0, i);
			source = newArray;
			
		}
		source[pos] = obj;
		return source;
	}

	public static long[] setIntoArray(long[] source, long value, int idxPos) {
		
		int i = source.length;
		if (idxPos>=i) {
			int newSize = i>0 ? i*2 : 2;
			if (idxPos>=newSize) {
				newSize = idxPos>0 ? idxPos*2 : 2;
			}
			long[] newArray = new long[newSize];
			System.arraycopy(source, 0, newArray, 0, i);
			source = newArray;
			
		}
		source[idxPos] = value;
		return source;
	}

	public static int[] setIntoArray(int[] source, int value, int pos) {
		
		int i = source.length;
		if (pos>=i) {
			int newSize = i>0 ? i*2 : 2;
			if (pos>=newSize) {
				newSize = pos>0 ? pos*2 : 2;
			}
			int[] newArray = new int[newSize];
			System.arraycopy(source, 0, newArray, 0, i);
			source = newArray;
			
		}
		source[pos] = value;
		return source;
	}	
	
	public static <T> T[] setIntoArray(T[] source, T obj, int pos) {
		
		int i = source.length;
		if (pos>=i) {
			int newSize = i>0 ? i*2 : 2;
			if (pos>=newSize) {
				newSize = pos>0 ? pos*2 : 2;
			}
			T[] newArray = (T[]) Array.newInstance(obj.getClass(), newSize);
			System.arraycopy(source, 0, newArray, 0, i);
			source = newArray;
			
		}
		source[pos] = obj;
		return source;
	}

	
	
	
}
