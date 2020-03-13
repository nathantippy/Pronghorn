package com.javanut.pronghorn.util;

import org.junit.Test;

public class HistogramLongPositTest {

	
	@Test
	public void testHist() {
		
		HistogramLongPosit h = new HistogramLongPosit(18,4);
		
		
		HistogramLongPosit.record(h, 10);
		
		int x = 5;
		while (--x>=0) {
			HistogramLongPosit.record(h, 200);
			HistogramLongPosit.record(h, 202);
			HistogramLongPosit.record(h, 204);
		}
		
		HistogramLongPosit.record(h, 1040);
		HistogramLongPosit.record(h, 1041);
				
		
		long center = HistogramLongPosit.spikeCenter(h,null,2d);
		
		
		
		System.out.println(h.toString()+"  center:"+center);
		
		
		
	}
	
}
