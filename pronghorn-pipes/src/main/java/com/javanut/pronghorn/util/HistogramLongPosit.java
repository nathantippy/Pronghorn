package com.javanut.pronghorn.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

public class HistogramLongPosit {

	private final int maxDetailsBits; //smaller buckets can find better peak, is there a better way?
	
	private final int[][] buckets = new int[64][];
	private final long[][] sums   = new long[64][]; 
		
	private long totalCount;
	private long maxValue;
	
	public HistogramLongPosit() {
		this.maxDetailsBits = 22;
	}
	
	public HistogramLongPosit(int maxDetailsBits) {
		this.maxDetailsBits = maxDetailsBits;
	}
	
	public String toString() {
		return report(new StringBuilder()).toString();
	}
	
	public static long totalCount(HistogramLongPosit that) {
		return that.totalCount;
	}
	
	public <A extends Appendable> A report(A target) {
		try {
			Appendables.appendValue(target.append("Total:"), totalCount).append("\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .25f)).append(" 25 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .50f)).append( " 50 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .80f)).append( " 80 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .90f)).append( " 90 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .95f)).append( " 95 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .98f)).append( " 98 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .99f)).append( " 99 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .999f)).append( " 99.9 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .9999f)).append( " 99.99 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .99999f)).append( " 99.999 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, .999999f)).append( " 99.9999 percentile\n");
			Appendables.appendValue(target, HistogramLongPosit.elapsedAtPercentile(this, 1f)).append( " max\n");
			
			for(int b = 0; b<64; b++) {
				int len = null==this.buckets[b] ? 0 : this.buckets[b].length;
				if (len>0) {
					System.out.println("bucket "+b+" floor "+(1L<<(Math.max(0, b-1)))+" array len "
				           + len
				           + " sum "
							+ (null==this.buckets[b] ? 0 : IntStream.of(this.buckets[b]).sum()  )
						   + " max "
							+ (null==this.buckets[b] ? 0 : IntStream.of(this.buckets[b]).max()  )
							);
					
					
					
					
				}
				
			}
			
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return target;
	}
	
	//TODO: auto combine until we see a peak. method on histogram
	
	//if histogram is flat eg only 0 or 1 found in each bucket, then half the resolution until we find a peak forming of peakGoal
	public static void compact(HistogramLongPosit that, int peakGoal) {
		
		//count them all if we find peakGoal quit, if we can never get to peak goal quit.
		boolean somethingWasCompacted = false;
		do {
			somethingWasCompacted = false;
			for(int x=0; x<that.buckets.length; x++) {
				if (that.sums[x]!=null) {
					int len = that.sums[x].length;
					for(int d=0; d<len; d++) {					
						if (that.buckets[x][d]>=peakGoal) {
							//System.out.println("found peak");
							return;//goal hit
						}
					}
					//goal was not hit on this level so compact it
					if (len>=2) { //only compact if we can
						//System.out.println("compacting "+len);
						somethingWasCompacted=true;
						int newLen = len/2;
						int[] newBuckets = new int[newLen];
						long[] newSums = new long[newLen];
						int j = 0;
						int k = 0;
						while (j<newLen) {
							newBuckets[j] = that.buckets[x][k] + that.buckets[x][k+1];
							newSums[j]    = that.sums[x][k]    + that.sums[x][k+1];
							j++;
							k+=2;
						}
						//if (len<=32) {
						//	System.out.println(Arrays.toString(newBuckets)+" vs "+Arrays.toString(that.buckets[x]));
						//}
						that.buckets[x] = newBuckets;
						that.sums[x] = newSums;
					}
					
				}
			}
		} while( somethingWasCompacted);
		
		
	}
	
	
	public static long spikeCenter(HistogramLongPosit that) {
		
		long maxBucketCount = 0;
		int runLength = 0;
		
		long num = 0;
		long den = 0;
		
		int c = 0;
		for(int x=0; x<that.buckets.length; x++) {
			if (that.sums[x]!=null) {
				for(int d=0; d<that.sums[x].length; d++) {
					c++;
					if (that.buckets[x][d]>0) {
						if (that.buckets[x][d] >= maxBucketCount) {
							//find the biggest bucket
							if (that.buckets[x][d] > maxBucketCount) {
								maxBucketCount = that.buckets[x][d];
								runLength = 0;//force replace at bottom.
							}
							
							long runNum = that.sums[x][d];
							int runDen = that.buckets[x][d];
							int runCount = 1;
							int x1 = x;
							int d1 = d;
							
							do {
								while (null!= that.sums[x1] && ++d1<that.sums[x1].length) {
									if (maxBucketCount == that.buckets[x1][d1]) {
										runCount++;	
										
										runNum += that.sums[x1][d1];
										runDen += that.buckets[x1][d1];
									} else {
										x1 = that.buckets.length;
										break;
									}									
								}
								d1=0;								
							} while (++x1<that.buckets.length);
							
							if (runCount>=runLength) {// equals is included since these are larger values.
								runLength = runCount;								
								num = runNum;
								den = runDen;
								
							}
						} 
					}					
				}
			}
		}
		return 0==den ? 0 : num/den;
	}
	
	
	public static long raiseFloor(HistogramLongPosit that, double pct) {
		
		assert(pct<1);
		assert(pct>0);
		
		long maxBucketCount = 0;
		int maxIdxX=-1;
		int maxIdxD=-1;
		
		long result = 0;
		int x = that.buckets.length;
		while (--x>=0) {
			if (that.sums[x]!=null) {
				int d = that.sums[x].length;
				while (--d>=0) {
						if (that.buckets[x][d] > maxBucketCount) {
							maxBucketCount = that.buckets[x][d];
							maxIdxX = x;
							maxIdxD = d;
							result = that.sums[maxIdxX][maxIdxD]/Math.max(1, that.buckets[maxIdxX][maxIdxD]);
						}
				}				
			}
		}
		
		long newFloor = (long) (maxBucketCount*pct);
	
		int total = 0;
		int values = 0;
		x = that.buckets.length;
		while (--x>=0) {
			if (that.sums[x]!=null) {					
				int d = that.sums[x].length;
				while (--d>=0) {
						if (that.buckets[x][d]>0) {
							total++;
							if (that.buckets[x][d]<newFloor) {
								that.totalCount-=that.buckets[x][d];
								that.buckets[x][d] = 0;
								that.sums[x][d] = 0;	
							} else {
								
								values++;
							}
						}
				}
			
			}
		}
		//System.out.println("buckets before filter "+total+" buckets after filter:"+values);
		return result;
	}
	
	public static void record(HistogramLongPosit that, long value) {
		
		value = Math.abs(value);
		
		int base = 64 - Long.numberOfLeadingZeros(value);
		
		int bits = 0;
		if (that.sums[base]==null) {
			final long floor = base<=0 ? 0 : (1L<<(base-1));         // 256 also size of window
			final long ceil =  (1+base)<=0 ? 0 : (1L<<((1+base)-1)); // 512
			
			bits = Math.min(that.maxDetailsBits, base<=0 ? 0 : (base-1));
			
			that.sums[base] = new long[1<<bits];
			that.buckets[base] = new int[1<<bits];
			
			//System.out.println("build new block of bits size: "+bits);
						
		} else {
			bits = Math.min(that.maxDetailsBits, base<=0 ? 0 : (base-1));
			
			
		}
		
		//take high detailsBits bits.. they all start with 1 so we must grab that one to skipit.
		int localBucketIdx = ( (1<<bits)-1 ) & (int)(value>>Math.max(0, base-(bits+1)));
				
		//System.out.println(Integer.toBinaryString(localBucketIdx)+" localBucketIdx "+localBucketIdx+" "+Long.toBinaryString(value));
		
		that.buckets[base][localBucketIdx]++;
		that.sums[base][localBucketIdx] += value;

		that.totalCount++;
		that.maxValue = Math.max(that.maxValue, value);
	}
	
	public static long elapsedAtPercentile(HistogramLongPosit that, double pct) {
		if (pct>1) {
			throw new UnsupportedOperationException("pct should be entered as a value between 0 and 1 where 1 represents 100% and .5 represents 50%");
		}		
		long targetCount = (long)Math.rint(pct * that.totalCount);
		if (targetCount==that.totalCount) {
			return that.maxValue;
		}
		
		if (0 != targetCount) {
			int i = 0;
			while (i<that.buckets.length-1) {
				
				if (null!=that.sums[i]) {
					
					int sumLimit = that.sums[i].length;
					int d = 0;
					while (d<sumLimit) {
					
						long sum = that.sums[i][d];
						int b = that.buckets[i][d++];
			
						if (targetCount<=b) {
						
							final long floor = i<=0 ? 0 : (1L<<(i-1));
							long step = floor/sumLimit;
							final long localFloor = floor + (((d-1)*floor)/sumLimit);
							
												
							long dif;
							int half = b>>1;
							if (half>1) {
								long avg = sum/(long)b;
								long center = avg-localFloor;
								assert(center>0) : "bad center: "+center+" avg "+avg+" localFloor "+localFloor+" d "+d+" floor "+floor;
								assert(center<=step);
								
								//weighted to the average
								if (targetCount<half) {
									dif = (center * targetCount)/half ;						
								} else {								
									dif = center+(( (step-center) * (targetCount-half) )/half) ;
								}
							} else {
								//this is the old linear less accurate solution
								dif = (floor * targetCount)/(long)(b*sumLimit) ; 
							}
							return Math.min(that.maxValue, localFloor + dif); 
							
						} else {
							targetCount -= b;				
						}
					}
				}
				i++;
			}
			return that.maxValue;		
		} else {
			return 0;
		}
	}

	
	
}
