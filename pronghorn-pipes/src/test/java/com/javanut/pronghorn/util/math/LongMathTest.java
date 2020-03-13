package com.javanut.pronghorn.util.math;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LongMathTest {

	@Test
	public void simpleTest() {
		
		LongMath math = new LongMath();
		
		math.sum(500);
		math.sum( 40);
		
		assertEquals(540, math.value());
		
		math.div(2);
		assertEquals(540/2, math.value());
		
		
		
	}
	
	@Test
	public void bitTest() {
		
		LongMath math = new LongMath();
		
		math.sum(500);
		math.sum( 40);
		math.sum(Long.MIN_VALUE);
		math.sum(-Long.MIN_VALUE);
		
		
		assertEquals(540, math.value());
		
		math.div(540);
		assertEquals(1, math.value());
				
		
	}
	
	
}
