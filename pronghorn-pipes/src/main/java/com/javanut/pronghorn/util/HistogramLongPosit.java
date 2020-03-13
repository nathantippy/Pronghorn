package com.javanut.pronghorn.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.stream.IntStream;

import com.javanut.pronghorn.util.ma.RunningStdDev;
import com.javanut.pronghorn.util.math.LongMath;

public class HistogramLongPosit {

	private final int maxDetailsBits; //smaller buckets can find better peak, is there a better way?
	private final int minBits;
	
	private final int[][] buckets = new int[64][];   //TODO: these arrays of ints ?? replace with Lois design
	private final long[][] sums   = new long[64][]; 
		
	private long totalCount;
	private long maxValue;
	
	public HistogramLongPosit() {
		this.maxDetailsBits = 22;
		this.minBits = 5;
	}
	
	public HistogramLongPosit(int maxDetailsBits, int minBits) {
		this.maxDetailsBits = maxDetailsBits;
		this.minBits = minBits;//how many values are totaled together at the smallest value
	}
	
	public String toString() {
		return report(new StringBuilder(), 1d).toString();
	}
	
	public static long totalCount(HistogramLongPosit that) {
		return null==that?0:that.totalCount;
	}
	
	public <A extends Appendable> A report(A target, double div) {
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
					long floor = 1L<<(Math.max(0, b-1));
					System.out.println("bucket "+b+" floor "+floor+" array len "
				           + len
				           + " sum "
							+ (null==this.buckets[b] ? 0 : IntStream.of(this.buckets[b]).sum()  )
						   + " max "
							+ (null==this.buckets[b] ? 0 : IntStream.of(this.buckets[b]).max()  )
							+ adjFloor(floor, div)
							);					
				}
				
			}
			
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return target;
	}
	
	private String adjFloor(long floor, double div) {
		if (div<=1) {			
			return "";
		} else {
			DecimalFormat df = new DecimalFormat("#.##########");
			return " actual: "+ df.format(floor/div);
		}
		
	}

	public RunningStdDev stdDevBucket() {
		
		RunningStdDev sd = new RunningStdDev();
		for(int b = 0; b<64; b++) {
			if (null!=buckets && null!=buckets[b]) {
				IntStream.of(buckets[b]).forEach(i->{
					RunningStdDev.sample(sd, i);		
				});
			}
		}
		return sd;
	}
	
	
	public long findLargestValueAtOrAboveLimit(int limit) {
		
		int b = 64;
		while (--b>=0) {
			int[] localBuckets = buckets[b];
			if (null!=localBuckets) {
				int x = localBuckets.length;
				while (--x>=0) {
					if ((localBuckets[x]>=limit)
							&& (x>=1 
							    ? localBuckets[x-1]<localBuckets[x] 
							    : ( b>=1 && buckets[b-1]!=null  
							         ? buckets[b-1][buckets[b-1].length-1]<localBuckets[x]  
							         : true ))
							) {					
						System.out.println("largest value at "+b+" "+x+"    "+(sums[b][x]/(long)buckets[b][x])+"="+sums[b][x]+"/"+(long)buckets[b][x]);
						return sums[b][x]/(long)buckets[b][x];						
					}
				}
			}
		}
		
		return -1;
		
	}
	
		
	public static HistogramLongPosit merge(HistogramLongPosit a, HistogramLongPosit b) {
		
		if (null==a) {
			return b;
		} if (null==b) {
			return a;			
		} else {
			
			HistogramLongPosit result = new HistogramLongPosit(Math.max(a.maxDetailsBits, b.maxDetailsBits), Math.max(a.minBits,b.minBits));
			
			result.maxValue = Math.max(a.maxValue, b.maxValue);
			result.totalCount = (a.totalCount + b.totalCount);
			
			for(int x=0; x<result.buckets.length; x++) {
				
				
				if (a.sums[x]==null) {
					
					result.sums[x] = b.sums[x];
					result.buckets[x] = b.buckets[x];
					
				} else if (b.sums[x]==null) {
										
					result.sums[x] = a.sums[x];
					result.buckets[x] = a.buckets[x];
					
				} else {		
									
					
					while (a.sums[x].length > b.sums[x].length) {						
						halfTheBucketsRun(a, x);
					}
					
					while (b.sums[x].length > a.sums[x].length) {						
						halfTheBucketsRun(b, x);
					}
					
					//both are now the same length, so combine...
					
					int len = a.sums[x].length;
					
					long[] aSum = a.sums[x];
					long[] bSum = b.sums[x];
					result.sums[x] = new long[len];
					for(int i=0; i<len; i++) {
						result.sums[x][i] = aSum[i] + bSum[i];						
					}	

					int[] aBuckets = a.buckets[x];
					int[] bBuckets = b.buckets[x];					
					result.buckets[x] = new int[len];
					for(int i=0; i<len; i++) {
						result.buckets[x][i] = aBuckets[i] + bBuckets[i];						
					}				
					
				}
			}
			
			return result;
		}

	}
	
	
	public static final int FLAG_UN_COMPRESSED = 1<<1;
	public static final int FLAG_FINISHED  = 1<<0;
	
	
	//if histogram is flat eg only 0 or 1 found in each bucket, then half the resolution until we find a peak forming of peakGoal
	public static void compact(HistogramLongPosit that, int peakGoal) {
		
		int flags = FLAG_UN_COMPRESSED;
		do {
			flags = combineNeighbors(that, peakGoal);
		} while( 0==flags );
		
	}

	public static int combineNeighbors(HistogramLongPosit that, int peakGoal) {
		int flags = FLAG_UN_COMPRESSED;
		if (null!=that && null!=that.buckets) {
			for(int x=0; x<that.buckets.length; x++) {
				if (that.sums[x]!=null) {
					
					final int len = that.sums[x].length;
					
					for(int d=0; d<len; d++) {					
						if (that.buckets[x][d]>=peakGoal) {
						
							flags |= FLAG_FINISHED;
							break;		//System.out.println("found peak");						
						}
					}
					//goal was not hit on this level so compact it
					if (len>=2) { //only compact if we can
						//System.out.println("compacting "+len);
				
						flags = flags & FLAG_FINISHED;
						halfTheBucketsRun(that, x);
					} else {
						//can not combine any further without causing unbalanced results
						flags |= FLAG_FINISHED;
						break;	
					}
				}
			}
		}
		return flags;
	}

	private static void halfTheBucketsRun(HistogramLongPosit that, int x) {
		int len = that.sums[x].length;
		int newLen = len>>1;
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

		that.buckets[x] = newBuckets;
		that.sums[x] = newSums;
	}
	
	public static long spikeCenter(HistogramLongPosit that) {
		return spikeCenter(that,null, 1d);
	}
	
	public static long spikeCenter(HistogramLongPosit that, LongMath[] target, double slopeWeight ) {
		
		LongMath maxBucketCount = new LongMath();
		int maxBucketBucketId = 0;
		
				
		int runLength = 0;
		
		long num = 0;
		long den = 0;
		int selX = -1;
		int selD = -1;
		
		int floor = 1;
		
		
		for(int x=0; x<that.buckets.length; x++) {
			double adjRun = Math.pow(slopeWeight, x);
			
			//System.out.println("adj: "+adjMax+"  "+adjRun);
			
			if (that.sums[x]!=null) {
				for(int d=0; d<that.sums[x].length; d++) {
					
					int selectedX = -1;
					int selectedD = -1;
					
					int runDen = that.buckets[x][d];
					if (runDen > 0) {
												
						double adjMax = Math.pow(slopeWeight, maxBucketBucketId);
						
						//System.out.println("run den: "+runDen+"   "+adjRun+"   "+maxBucketCount.value()+"   "+adjMax);
						
						if ( (runDen>floor) && ((runDen * adjRun) >= (maxBucketCount.value() * adjMax)) ) {
							//find the biggest bucket
							if ( (runDen * adjRun) > (maxBucketCount.value() * adjMax) ) {
								maxBucketCount = new LongMath(runDen);
								runLength = 0;//force replace at bottom.
								maxBucketBucketId = x;
								
							}
						
							long runNum = that.sums[x][d];
							int runCount = 1;
							int x1 = x;
							int d1 = d;
							
							selectedX = x;
							selectedD = d;
							//System.out.println("center found at: "+x+"  "+d+"  result: "+(runNum/(long)runDen));
							
							do {
								while (null!= that.sums[x1] && ++d1<that.sums[x1].length) {
									if (maxBucketCount.value() == (that.buckets[x1][d1]) ) {
										runCount++;
										
										
										runNum += that.sums[x1][d1];
										runDen += that.buckets[x1][d1];
										
										//System.out.println("center found at: "+x1+"  "+d1+"  result: "+(runNum/(long)runDen));
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
								selX = selectedX;
								selD = selectedD;
							}
						} 
						
						
						
					}
				}
			}
		}
		
		long result =  0==den ? 0 : num/den;
		
		//System.out.println("   final center   result: "+(result)+"="+num+"/"+den+"  "+selX+"  "+selD);
		
		if (null != target) {
			
			if (maxBucketCount.value() > target[0].value()) {
				target[0] = maxBucketCount; 
				target[1] = new LongMath(result);
				
			}
			
		}
		
		return result;
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
			
			bits = Math.min(that.maxDetailsBits, base<=that.minBits ? 0 : (base-that.minBits));
			
			that.sums[base] = new long[1<<bits]; //TODO: build short array first then grow as needed.
			that.buckets[base] = new int[1<<bits];
			
			//System.out.println("build new block of bits size: "+bits);
						
		} else {
			bits = Math.min(that.maxDetailsBits, base<=that.minBits ? 0 : (base-that.minBits));
			
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
								assert(center>=0) : "bad center: "+center+" avg "+avg+" localFloor "+localFloor+" d "+d+" floor "+floor;
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
