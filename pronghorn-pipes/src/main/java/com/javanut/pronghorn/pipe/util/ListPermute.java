package com.javanut.pronghorn.pipe.util;

import java.util.List;
import java.util.function.Consumer;

public class ListPermute {

	public static <T> void heapPermute(List<T> array, Consumer<List<? super T>> consumer ) {
		heapPermute(array.size(), array, consumer);
	}
	
	private static <T> void heapPermute(int n, List<T> array, Consumer<List<? super T>> consumer ) {
		  if (n == 1) {
			  consumer.accept(array);
		  } else {
		    for (int i = 0; i < n; i++) { 
		      heapPermute(n-1, array, consumer);
		      if (n % 2 == 1) { // if n is odd
		        swap(0, n-1, array);
		      } else {            // if n is even
		        swap(i, n-1, array);
		      }
		    }
		  }
		}	  
	  
	  private static <T> void swap(int i, int j, List<T> array) {
		  T temp = array.get(i);
		  array.set(i, array.get(j));
		  array.set(j, temp);
	  } 
	  
	  
}
