package com.javanut.pronghorn.util.search;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.javanut.pronghorn.util.search.KnuthMorrisPratt;
import com.javanut.pronghorn.util.search.MatchVisitor;

public class KnuthMorrisPrattTest {

	
    @Test
    public void simpleSearchTest() {
    	  KMPSearch("ABABCABAB",
    			    "ABABDABACDABABCABAB", 
    			    10);
    
    }
    
    
	private static void KMPSearch(String pat, String txt, final int expected) {
		 
		    
		 	final AtomicInteger count = new AtomicInteger();
		 	
		 	byte[] patBytes = pat.getBytes();
		 	byte[] txtBytes = txt.getBytes();
		 	
	        int patLen = patBytes.length;
	        int txtLen = txtBytes.length;

	        int jumpTable[] = new int[patLen];
	
	        KnuthMorrisPratt.populateJumpTable(patBytes,0,patLen,Integer.MAX_VALUE,jumpTable);
	 
	        MatchVisitor visitor = new MatchVisitor() {

				@Override
				public boolean visit(int position) {
					assertEquals(position, expected);
					count.incrementAndGet();
					return false;
				}
	        	
	        };
	        
	        KnuthMorrisPratt.search(
	        	   patBytes, 0, patLen, Integer.MAX_VALUE,
	        	   txtBytes,  0, txtLen, Integer.MAX_VALUE,
	        	   jumpTable, visitor);
	        
	        assertEquals(1, count.get());
	        
	    }
	 
	 
	 
	 
	 
}
