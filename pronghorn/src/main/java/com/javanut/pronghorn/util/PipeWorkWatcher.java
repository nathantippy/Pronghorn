package com.javanut.pronghorn.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipePublishListener;

public class PipeWorkWatcher {

	private int groupBits;
	private int groups;

    private int step;
    private int length;
    
    private AtomicInteger[] groupVersion;
    private AtomicLong workFlags = new AtomicLong();
    
    public PipeWorkWatcher() {
    }
    
    public static boolean hasWork(PipeWorkWatcher pww) {
    	return 0 != pww.workFlags.get();
    }
    
    public int groups() {
    	assert(groups>=1) : "internal error, must init before use";
    	return groups;
    }
    
    
	public void init(Pipe[] inputs) {
		
		if (inputs.length >= 256) {
			   groupBits = 6;  //64 groups absolute max
		} else {		
			   if (inputs.length >=64 ) {
				   groupBits = 2;
			   } else {
				   groupBits = 0;
			   }
			
		}
		assert(groupBits<=6);//group bits may not be larger since we use long for mask...
		
		groups = 1<<groupBits;		
		length = inputs.length;
			
		groupVersion = new AtomicInteger[groups];
		int r = groups;
		while (--r >= 0) {
			groupVersion[r] = new AtomicInteger();
		}
		
        int i = inputs.length;
        step = (int)Math.ceil(i/(float)groups);
        if (step<=1) {
        	step = i+1;
        }
        
        
        while (--i >= 0) {
        	    	
        	final int h = i;
        	final int g = i/step;
        	assert(g<groups) :"internal error";
        	
        	PipePublishListener listener = new PipePublishListener() {
	    		@Override
	    		public void published(long workingHeadPosition) {		    			
	    			
	    			//must bump version since we moved the head.
	    			groupVersion[g].incrementAndGet();
	    			
    				final long value = 1L<<g;
    				long old;
    				do {
    					old = workFlags.get();
    					if ((value&old)!=0) {
    						break;
    					}
    				} while(!workFlags.compareAndSet(old, old | value));
	    		
	    		}
	        };
	        
        	Pipe.addPubListener(inputs[i], listener);
        }
        
	}
	

	public static int getStartIdx(PipeWorkWatcher pww, int g) {
		return g*pww.step;
	}
	
	public static int getLimitIdx(PipeWorkWatcher pww, int g) {
		return Math.min((g+1)*pww.step, pww.length);
	}

	public static int version(PipeWorkWatcher pww, int g) {
		return pww.groupVersion[g].get();
	}

	public static boolean hasNoWork(PipeWorkWatcher pww, int g) {
		return 0 == (pww.workFlags.get() & (1L<<g));
	}

	public static boolean hasWork(PipeWorkWatcher pww, int g) {
		return 0 != (pww.workFlags.get() & (1L<<g));
	}

	public static void clearScanState(PipeWorkWatcher pww, int g, int version) {
		if (version == version(pww, g)) {
			boolean ok = true;
			do {
				long old = pww.workFlags.get();
				ok = pww.workFlags.compareAndSet(old, old & (~(1L<<g)));					
			} while(!ok);
							
			if (version != version(pww, g)) {
				//oops we must put this back
				ok = true;
				do {
					long old = pww.workFlags.get();
					ok = pww.workFlags.compareAndSet(old, old | (1L<<g));
					
				} while(!ok);
			}
		}
	}

    
}
