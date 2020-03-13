package com.javanut.pronghorn.util;

import com.javanut.pronghorn.util.math.LongMath;

public class HistogramLongPosit2D {

	private final int maxDetailsBits;
	
	private final HistogramLongPosit[][] buckets = new HistogramLongPosit[64][];
	
	private LongMath[][] sums = new LongMath[64][];
	
	
	public HistogramLongPosit2D() {
		this.maxDetailsBits = 22;
	}
	
	public HistogramLongPosit2D(int maxDetailsBits) {
		this.maxDetailsBits = maxDetailsBits;
	}
	
	
	public static void record(HistogramLongPosit2D that, long valueA, long valueB) {
		
		valueA = Math.abs(valueA);		
		int baseA = 64 - Long.numberOfLeadingZeros(valueA);		
		int bits = Math.min(that.maxDetailsBits, baseA<=0 ? 0 : (baseA-1));
		int localBucketIdx = ( (1<<bits)-1 ) & (int)(valueA>>Math.max(0, baseA-(bits+1)));		
		
		
		HistogramLongPosit[] t = that.buckets[baseA];
		if (null == t) {
			t = new HistogramLongPosit[1<<bits];
			that.buckets[baseA] = t;
		}
		if (null == t[localBucketIdx]) {
			t[localBucketIdx] = new HistogramLongPosit(that.maxDetailsBits,5);
		}
		
		HistogramLongPosit.record(t[localBucketIdx], valueB);
		
		
		if (that.sums[baseA] == null) {	
			//that.sums[baseA] = new LongMath[1<<bits];
			that.sums[baseA] = new LongMath[1];
			that.sums[baseA][0] = new LongMath(valueA); //just store 1 for now			
			return;
		} 

		
		LongMath[] sumArray = that.sums[baseA];		
		
		if ( (sumArray.length<(1<<bits))  ) {
			//must grow array 
		
			LongMath[] newSums = new LongMath[1<<bits]; //the original desired size
			int x = sumArray.length;
			while (--x>=0) {
				if (null!=sumArray[x]) {
					long value = sumArray[x].value();
					int targetBase = 64 - Long.numberOfLeadingZeros(value);		
					int targetBits = Math.min(that.maxDetailsBits, targetBase<=0 ? 0 : (targetBase-1));
					int targetBucketIdx = ( (1<<targetBits)-1 ) & (int)(value>>Math.max(0, targetBase-(targetBits+1)));	
					
					newSums[targetBucketIdx] = sumArray[x];
				}
			}
			sumArray = newSums;
			that.sums[baseA] = newSums;			
		}		
		
		LongMath orig = sumArray[localBucketIdx];
		if (null==orig) {
			sumArray[localBucketIdx] = new LongMath(valueA);			
		} else {
			sumArray[localBucketIdx] = orig.clone().sum(valueA);
		}

		
	}
	
	public static void compact(HistogramLongPosit2D that, int peakGoal) {
		
		//TODO: this is breaking the results, not sure why.
		
		int flags = 0; 
		int max = 64;
		do {
			
			//
			//compact A ...   this is messing up the rolling data..
			//
			int x = 64;
			while (--x>=0) {			
				if (null != that.buckets[x]) {				
					
					HistogramLongPosit[] histBuckets = that.buckets[x];
					LongMath[] sums = that.sums[x];
					
					if (histBuckets.length>2) {
						
						HistogramLongPosit[] histBucketsNew = new HistogramLongPosit[histBuckets.length>>1];	
						LongMath[] sumsNew  = new LongMath[sums.length>>1];
						int i = 0;
						while (i < histBuckets.length) {					
							HistogramLongPosit mergedHisto = null;
							LongMath mergedSums = null;
							
							if (null == histBuckets[i]) {
								
								mergedHisto = histBuckets[i+1];
								mergedSums = sums[i+1];
								
							} else if (null == histBuckets[i+1]) {
								
								mergedHisto = histBuckets[i];	
								mergedSums = sums[i];
							
							} else {
															
								//NOTE: when we merge the new length may be shorter than we think?
								mergedHisto = HistogramLongPosit.merge(histBuckets[i], histBuckets[i+1]);	
								mergedSums = sums[i+1].clone().sum(sums[i]);								
							
							}
							
							histBucketsNew[i>>1] = mergedHisto;	
							sumsNew[i>>1] = mergedSums;
							
							i+=2;
						}	
						
						that.buckets[x] = histBucketsNew;
						that.sums[x] = sumsNew;
					
						
					}
				}
			}
			
			//
			//compact B ...  this part works..
			//
			flags = 0;
			x = 64;
			while (--x>=0) {			
				if (null != that.buckets[x]) {				
					HistogramLongPosit[] histBuckets = that.buckets[x];
					
					
					int i = 0;
					while (i < histBuckets.length) {								
						flags |= HistogramLongPosit.combineNeighbors(histBuckets[i++], peakGoal);						
					}							
				}
			}
			
			//
			//check
			//
		} while (((flags & HistogramLongPosit.FLAG_FINISHED) != 0) && (--max>=0));
	
		
	}
	
	
	public static LongMath spikeCenter(HistogramLongPosit2D that) {
		
		LongMath[] target = new LongMath[3];
		target[0] = new LongMath();
		target[1] = new LongMath();
		target[2] = new LongMath();		
		
		spikeCenter(that, target);
		
		return target[1];
	
	}

	public static void spikeCenter(HistogramLongPosit2D that, LongMath[] target) {
		int x = 64;
		while (--x>=0) {			
			HistogramLongPosit[] histogramLongPosits = that.buckets[x];
			if (null != histogramLongPosits) {
				
				for(int i=0; i<histogramLongPosits.length; i++) {
				
					HistogramLongPosit h = histogramLongPosits[i];
				 
					if (null != h) {
						
						
						
						//TODO: how to get the other dimention in here??
						// sums/buckets for this peak which is the average value ....
						// which are included int this peek?
						long old = target[0].value();
						HistogramLongPosit.spikeCenter(h, target, 1d);
						if (target[0].value() != old) {
							
//							long floor = x<=0     ? 0 : (1L<<(x-1));         // 256 also size of window
//							long ceil =  (1+x)<=0 ? 0 : (1L<<((1+x)-1)); // 512
//							long dif = ceil-floor;
//							target[2] =  floor + ((i*dif)/histogramLongPosits.length);
														
							
							LongMath est =  that.sums[x][i].div(HistogramLongPosit.totalCount(h) ); 
							target[2] = est;
							
//							System.out.println("est value:"+est+"   \nsums:"+that.sums[x][i]+"  \ncount:"+HistogramLongPosit.totalCount(h));
//							
//							BigDecimal FREQ_SCALE = new BigDecimal(	10_0000_0000_0000L);//_0000L );
//									
//							System.out.println("floor:"+floor/FREQ_SCALE.doubleValue());
//							System.out.println("actualEst: "+est.divide(FREQ_SCALE));
//							System.out.println("ceil:"+ceil/FREQ_SCALE.doubleValue());
//							System.out.println();
//							
//							
////							System.out.println("sum: "+(that.sums[x][i])+"  total count: "+HistogramLongPosit.totalCount(h)+"     est:"+
////												est.divide(new BigDecimal(10_0000_0000_0000_0000L), MathContext.DECIMAL128)
////														+" expectedEst:"+expectedEst);
							
							
						}
						
						
					}
				}	
			}
		}
	}
	
	
}
