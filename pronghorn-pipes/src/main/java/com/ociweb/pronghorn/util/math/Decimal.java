package com.ociweb.pronghorn.util.math;

public class Decimal {

    public static void sum(final long aM, final byte aE, final long bM, final byte bE, DecimalResult result) {
    	
		if (aE==bE) {
			result.result(aM + bM, bE);
		} else if (aE>bE){
			int dif = (aE-bE);		
			//if dif is > 18 then we will loose the data anyway..  
			long temp =	dif>=longPow.length? 0 : aM*longPow[dif];
			result.result(bM + temp, bE);
		} else {
			int dif = (bE-aE);						
			long temp =	dif>=longPow.length? 0 : bM*longPow[dif];
			result.result(aM+temp, aE);
		}
    }
    
    private static final byte[] placesLookup = 
    	{(byte)0,(byte)-3,  (byte)-6,     (byte)-9,        (byte)-12,              (byte)-15,                   (byte)-18};
    private static final long[] multLookup   = 
    	{      1,    1000, 1_000_000,1_000_000_000,1_000_000_000_000L, 1_000_000_000_000_000L, 1_000_000_000_000_000_000L};
    

	public static void fromRational(long numerator, long denominator, DecimalResult result) {
		
		long ones = numerator/denominator;
		long rem  = numerator%denominator;
		
		//based on ones how much room do we have?
		//10 bits is 1024
		final int ks = Long.numberOfLeadingZeros(Math.abs(ones))/10;

		long mul    = multLookup[ks];
		long value = (ones*mul)+((rem*mul)/denominator);
	
		result.result(value, placesLookup[ks]);		
	}
    
	public static long[] longPow = new long[] {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 
            1_000_000_000, 10_000_000_000L, 100_000_000_000L ,1000_000_000_000L,
            1_000_000_000_000L, 10_000_000_000_000L, 100_000_000_000_000L ,1000_000_000_000_000L,
            1_000_000_000_000_000L, 10_000_000_000_000_000L, 100_000_000_000_000_000L ,1000_000_000_000_000_000L};
	
    public final static double[] powdi = new double[]{
        	1.0E64,1.0E63,1.0E62,1.0E61,1.0E60,1.0E59,1.0E58,1.0E57,1.0E56,1.0E55,1.0E54,1.0E53,1.0E52,1.0E51,1.0E50,1.0E49,1.0E48,1.0E47,1.0E46,1.0E45,1.0E44,1.0E43,1.0E42,1.0E41,1.0E40,1.0E39,1.0E38,1.0E37,1.0E36,1.0E35,1.0E34,1.0E33,
        	1.0E32,1.0E31,1.0E30,1.0E29,1.0E28,1.0E27,1.0E26,1.0E25,1.0E24,1.0E23,1.0E22,1.0E21,1.0E20,1.0E19,1.0E18,1.0E17,1.0E16,1.0E15,1.0E14,1.0E13,1.0E12,1.0E11,1.0E10,1.0E9,1.0E8,1.0E7,1000000.0,100000.0,10000.0,1000.0,100.0,10.0,
        	1.0,0.1,0.01,0.001,1.0E-4,1.0E-5,1.0E-6,1.0E-7,1.0E-8,1.0E-9,1.0E-10,1.0E-11,1.0E-12,1.0E-13,1.0E-14,1.0E-15,1.0E-16,1.0E-17,1.0E-18,1.0E-19,1.0E-20,1.0E-21,1.0E-22,1.0E-23,1.0E-24,1.0E-25,1.0E-26,1.0E-27,1.0E-28,1.0E-29,1.0E-30,1.0E-31,
        	0E-32,1.0E-33,1.0E-34,1.0E-35,1.0E-36,1.0E-37,1.0E-38,1.0E-39,1.0E-40,1.0E-41,0E-42,1.0E-43,1.0E-44,1.0E-45,1.0E-46,1.0E-47,1.0E-48,1.0E-49,1.0E-50,1.0E-51,1.0E-52,1.0E-53,1.0E-54,1.0E-55,1.0E-56,1.0E-57,1.0E-58,1.0E-59,1.0E-60,1.0E-61,1.0E-62,1.0E-63,1.0E-64
        };
        
    public final static float[] powfi = new float[]{
        	Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,1.0E38f,1.0E37f,1.0E36f,1.0E35f,1.0E34f,1.0E33f,
        	1.0E32f,1.0E31f,1.0E30f,1.0E29f,1.0E28f,1.0E27f,1.0E26f,1.0E25f,1.0E24f,1.0E23f,1.0E22f,1.0E21f,1.0E20f,1.0E19f,1.0E18f,1.0E17f,1.0E16f,1.0E15f,1.0E14f,1.0E13f,1.0E12f,1.0E11f,1.0E10f,1.0E9f,1.0E8f,1.0E7f,1000000.0f,100000.0f,10000.0f,1000.0f,100.0f,10.0f,
        	1.0f,0.1f,0.01f,0.001f,1.0E-4f,1.0E-5f,1.0E-6f,1.0E-7f,1.0E-8f,1.0E-9f,1.0E-10f,1.0E-11f,1.0E-12f,1.0E-13f,1.0E-14f,1.0E-15f,1.0E-16f,1.0E-17f,1.0E-18f,1.0E-19f,1.0E-20f,1.0E-21f,1.0E-22f,1.0E-23f,1.0E-24f,1.0E-25f,1.0E-26f,1.0E-27f,1.0E-28f,1.0E-29f,1.0E-30f,1.0E-31f,
        	0E-32f,1.0E-33f,1.0E-34f,1.0E-35f,1.0E-36f,1.0E-37f,1.0E-38f,1.0E-39f,1.0E-40f,1.0E-41f,0E-42f,1.0E-43f,1.0E-44f,1.0E-45f,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN
        };
	
    public static long asLong(final long m, final byte e) {
    	return (long)(((double)m)*powdi[64 - e]);
    }
    
    public static double asDouble(final long m, final byte e) {
    	return ((double)m)*powdi[64 - e];
    }
	
    public static float asFloat(final long m, final byte e) {
    	return ((float)m)*powfi[64 - e];
    }

	public static long asNumerator(long m, byte e) {
		return e<0 ? m : asLong(m, e);
	}

	public static long asDenominator(byte e) {
		return e<0 ? (long)(1d/powdi[64 - e]) : 1;
	}

	public static void asRational(long tempNumM, byte tempNumE,
			                      long tempDenM, byte tempDenE, 
			                      RationalResult rational) {
		
		//how many bits can we add?		
		int numZeros = Long.numberOfLeadingZeros(tempNumM);
		int denZeros = Long.numberOfLeadingZeros(tempDenM);		
		int minZeros = Math.min(numZeros, denZeros)-1;
		
		int tens = minZeros/10; //10 bits is 1024		
		long multi = multLookup[tens];
		int  places = placesLookup[tens];
		
		tempNumM*=multi;
		tempDenM*=multi;
		tempNumE-=places;
		tempDenE-=places;
		
		rational.result(Decimal.asLong(tempNumM,tempNumE), 
				        Decimal.asLong(tempDenM,tempDenE));
		
	}

	
}
