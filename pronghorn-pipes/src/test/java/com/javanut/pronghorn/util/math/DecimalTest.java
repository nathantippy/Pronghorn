package com.javanut.pronghorn.util.math;

import org.junit.Assert;
import org.junit.Test;

import com.javanut.pronghorn.util.math.Decimal;
import com.javanut.pronghorn.util.math.DecimalResult;

public class DecimalTest {

	
	@Test
	public void divideTest() {
		
		DecimalResult result = new DecimalResult() {
			@Override
			public void result(final long m1, final byte e1) {
				
				DecimalResult result2 = new DecimalResult() {

					@Override
					public void result(long m2, byte e2) {
						
						DecimalResult result3 = new DecimalResult() {

							@Override
							public void result(long m, byte e) {
								
								double actual = Decimal.asDouble(m, e);
								double expected = (-123d/7d)/(-12d/5d);
								Assert.assertEquals(expected, actual, .00000000001);
								
							}
						};
						Decimal.divide(m1, e1, m2, e2, result3);
					}
				};
				Decimal.fromRational(-12, 5, result2);
			}
		};
		Decimal.fromRational(-123, 7, result);
		
	}
	
	
	@Test
	public void multiplyTest() {
		
		DecimalResult result = new DecimalResult() {
			@Override
			public void result(final long m1, final byte e1) {
				
				DecimalResult result2 = new DecimalResult() {

					@Override
					public void result(long m2, byte e2) {
						
						DecimalResult result3 = new DecimalResult() {

							@Override
							public void result(long m, byte e) {
								
								double actual = Decimal.asDouble(m, e);
								double expected = (-123d/7d)*(12d/5d);
								Assert.assertEquals(expected, actual, .00000000001);
								
							}
						};
						Decimal.multiply(m1, e1, m2, e2, result3);
					}
				};
				Decimal.fromRational(12, 5, result2);
			}
		};
		Decimal.fromRational(-123, 7, result);
		
	}
	
	
	@Test
	public void sumAndRational() {
		
		
		DecimalResult result = new DecimalResult() {
			@Override
			public void result(final long m1, final byte e1) {
				
				DecimalResult result2 = new DecimalResult() {

					@Override
					public void result(long m2, byte e2) {
						
						DecimalResult result3 = new DecimalResult() {

							@Override
							public void result(long m, byte e) {
								
								//TODO: may want to revisit this math issue later..
								//NOTE: this actual sum is rounding up on the very last digit
								
								double actual = Decimal.asDouble(m, e);
								double expected = (-1235d/7d)+(34567d/5d);
								Assert.assertEquals(expected, actual, .00000000001);
								
							}
						};
						Decimal.sum(m1, e1, m2, e2, result3);
					}
				};
				Decimal.fromRational(34567, 5, result2);
			}
		};
		Decimal.fromRational(-1235, 7, result);
	}
	
	
}
