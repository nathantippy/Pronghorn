package com.javanut.pronghorn.util.math;

import java.util.Arrays;

public class LongMath {

	private int extraBits = 1;
	private int size      = 1<<extraBits;
	private int mask      = size-1;
	
	private long[] storage;
	private int iter;
	
	private final long maxNumer = Long.MAX_VALUE >> (1 + extraBits);
	
	public void clear() {
		iter = 0;
		Arrays.fill(storage, 0);	
	}
	
	private void grow() {
		
		this.extraBits *= 2;
		this.size      = 1<<extraBits;
		this.mask      = size-1;
				
		long[] newStorage = new long[size];
		System.arraycopy(storage, 0, newStorage, 0, storage.length);
		this.storage = newStorage;
		
	}
	
	//Hold an array of longs to spread out the value so we do not lose any accuracy
	//when summing up a bunch of longs.
	
	public LongMath() {
		storage = new long[size];		
	}
	
	public LongMath(long value) {
		storage = new long[size];
		sum(value);
	}
	
	public LongMath clone() {
		LongMath result = new LongMath();
		result.iter = this.iter;
		System.arraycopy(storage, 0, result.storage, 0, size);		
		return result;
	}
	

	public LongMath sum(LongMath longMath) {
		
		int x = longMath.storage.length;
		while (--x>=0) {
			sum(longMath.storage[x]);	
		}
		return this;
	}
	
	
	public LongMath sum(long value) {
		if (storage[mask & iter] > maxNumer ) {
			grow();
		}
		
		storage[mask & iter++] += value;
		return this;
	}
	
	public long value() {
		long result = 0;
		int x = size;
		while (--x>=0) {
			result += storage[x];
		}
		return result;
	}
	
	public double valueDouble() {
		double result = 0;
		int x = size;
		while (--x>=0) {
			result += storage[x];
		}
		return result;
	}

	
	public LongMath div(long value) {
		
		long sum = 0;
		int x = size;
		int lastPos = -1;
		while (--x>=0) {
			
			if (storage[x] < maxNumer) {
				sum += storage[x];
				
				if (sum > maxNumer) {
					storage[x] = sum/value;
					sum = 0;
				} else {
					storage[x] = 0;
				}	
				lastPos = x;
			} else {
				storage[x] = storage[x]/value;
			}
		}
		if (sum!=0) {
			storage[lastPos] = sum/value;
			sum = 0;
		}
		return this;
	}

	
}
