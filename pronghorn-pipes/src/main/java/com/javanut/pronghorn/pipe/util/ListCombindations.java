package com.javanut.pronghorn.pipe.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ListCombindations {

	 
	public static void main(String arg[]) {

	 
	    ArrayList<String> list = new ArrayList<String>();
	    
	    //16 
	    //1890207555
	    //min: 38
	    
	    //18    24 hrs not done.
	    
	    int x = 16; 
	    while (--x>=0) {
	    	list.add(""+(char)('A'+x));
	    }
	   	    
	    Comparator<String> comp = new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}	    	
	    };
	      
	    Map<String,List<String>> cache1 = new HashMap<String, List<String>>();
	    Map<String,Map<String,List<String>>> cache2 = new HashMap<String, Map<String, List<String>>>();
	    
	    AtomicLong rowCount = new AtomicLong();
	    
	    long now = System.currentTimeMillis();
	    iterateCombinations(list, new ArrayList<List<String>>(), list.size(), null, comp, cache1, cache2, (v)->{
		    		
	    			long count = rowCount.getAndIncrement();
	    			
	    			if (count>200) {
	    				if (0 == (0x3FFFFFL&count)) {
	    					System.out.println(v);	
	    				}
	    			} else {
	    				System.out.println(v);
	    			}
		    
	    	
		    		return true;
	    });
	    System.out.println(count);
	    
	    long duration = System.currentTimeMillis()-now;
	    
	    long min = duration/(60_000L);
	    System.out.println("min: "+min);
	    
	}
	
	public static AtomicInteger count = new AtomicInteger();
	
	
	public static <T> boolean iterateCombinations(List<T> list, List<List<T>> result, 
			                                     int maxSize, T groupHead, Comparator<T> comp, 
			                                     Map<T,List<T>> cache1, Map<T,Map<T,List<T>>> cache2,
			                                     VisitCombinations<List<List<T>>> vc) {
		
		
		AtomicBoolean okContinue = new AtomicBoolean(true);
		int itemCount = list.size();		
	
		int s = Math.min(itemCount,maxSize)+1;
		while (--s > 0 ) {		
			
			 
			 final int len = s;
			 visitAll(itemCount, len,
					 (v)-> {
						 						 
						    if ( v.length<maxSize || groupHead==null || comp.compare(list.get(v[0]), groupHead)<0) {
						    
							     //TODO: turn these into persitant structs.
								 List<List<T>> newResult = new ArrayList<List<T>>(result);
								 List<T> newList = new ArrayList<T>(list); 
								 
								 T first = null;
								 if (len==1) {
									 							
									 newResult.add(buildSingle(cache1, newList.remove(v[0])));
									 									 
								 } else {
									 
									 if (len==2) {
										 
										 T second = newList.remove(v[1]);
										 first = newList.remove(v[0]);
										 
										 List<T> newGroup = buildDouble(cache2, first, second);
										 							 
										 newResult.add(newGroup);
										 
									 } else {
									 
									 
										 /////
										 List<T> newGroup = new ArrayList<T>(len);
										 int x = len;
										 while (--x >= 0) {							
											 newGroup.add( newList.remove(v[x]) );
										 }								 
										 newResult.add(newGroup);
										 first = newGroup.get(newGroup.size()-1);
									 }
								 }
								 
								
								 ////
								 if (!newList.isEmpty()) {
									 
									 
									 if (len==1) {
									
										 for(T item: newList) {
											 
											 newResult.add(buildSingle(cache1, item));
											 
										 }
										 
										 //len was the full list size so we have reached the bottom.								 
										 if (!vc.visit(newResult)) {
											 okContinue.set(false);
											 return false;
										 };
										 count.getAndIncrement();
										 
										 //stop on all 1's
										 int n = newResult.size();
										 int ones = 0;
										 while(--n >= 0) { //if first is not 1 that is ok we will still skip the rest.
											 if (newResult.get(n).size()!=1) {
												 break;
											 } else {
												 ones++;
											 }
										 }	
										 if (n<0) {
											 //we are at the bottom so stop.
											 okContinue.set(false);
											 return false;
										 }	
										 return false;
										 
									 } else {

																				 
										 
										 if (!iterateCombinations(newList, newResult, len, first, comp, cache1, cache2, vc)) {
											 okContinue.set(false);
											 return false;
										 }
										 
									 }
									 
								 } else {
									 //len was the full list size so we have reached the bottom.								 
									 if (!vc.visit(newResult)) {
										 okContinue.set(false);
										 return false;
									 };
									 count.getAndIncrement();
										 
								 }
								 
	
									//TODO: happens in many other cases as well.
								 if (len==1 && itemCount==2 ) {
									 //when we have 2 groups of 1 we can get both groups but we only need one.
									return false;
								 }
						    } 
							 return true;
			    });
		}		
		return okContinue.get();
	}



	private static <T> List<T> buildDouble(Map<T,Map<T,List<T>>> cache2, T first, T second) {
		
		Map<T,List<T>> temp = cache2.get(second);
		if (temp==null) {
			temp = new HashMap<T,List<T>>();
			cache2.put(second,temp);
		}
		List<T> result = temp.get(first);
		if (null==result) {
			result = new ArrayList<T>();
			result.add(second);
			result.add(first);
			temp.put(first, result);			
		}
		return result;
		
	}



	private static <T> List<T> buildSingle(Map<T, List<T>> cache, T item) {
		List<T> t =cache.get(item);
		 if (null==t) {
			 t = new ArrayList<T>(1);
			 t.add(item);
			 cache.put(item, t);
		 }
		return t;
	}
	
	
	
    public static long factorial(int number) {
        long result = 1;
        for (int factor = 2; factor <= number; factor++) {
            result *= factor;
        }
        return result;
    }
	
	
	
	/*
	    next_comb(int comb[], int k, int n)
	        Generates the next combination of n elements as k after comb
	 
	    comb => the previous combination ( use (0, 1, 2, ..., k) for first)
	    k => the size of the subsets to generate
	    n => the size of the original set
	 
	    Returns: 1 if a valid combination was found
	        0, otherwise
	*/
	private static boolean next(int comb[], int k, int n) {
	    int i = k - 1;
	    ++comb[i];
	    while ((i > 0) && (comb[i] >= n - k + 1 + i)) {
	        --i;
	        ++comb[i];
	    }
	 
	    if (comb[0] > n - k) {/* Combination (n-k, n-k+1, ..., n) reached */
	        return false; /* No more combinations can be generated */
	    }
	        
	    /* comb now looks like (..., x, n, n, n, ..., n).
	    Turn it into (..., x, x + 1, x + 2, ...) */
	    for (i = i + 1; i < k; ++i) {
	        comb[i] = comb[i - 1] + 1;
	    }
	 
	    return true;
	}


	public static void visitAll(int n, int k, VisitCombinations<int[]> vc) {
		AtomicLong c = new AtomicLong();
	
		
		int[] comb = new int[k];	//TODO: cache this with local cache.. 
	    for (int i = 0; i < k; ++i) {
	        comb[i] = i;
	    }
	 
	    do {
	     	c.getAndIncrement();
	    	if (!vc.visit(comb)) {
	    		return;
	    	};
	    } while (next(comb, k, n));
	    

//	     n count of items
//	     r items per group 	       
//	     n! / (r! (n - r)!)  how many ways can a group of r be selected from n 
	     assert(c.get() == (factorial(n) / (factorial(k) * factorial(n-k)))) : "expected "+(factorial(n) / (factorial(k) * factorial(n-k)))+" but only visited "+c;
	     //System.out.println(c+" vs "+(factorial(n) / (factorial(k) * factorial(n-k))));

	    
	}
	
	
}
