package com.javanut.pronghorn.stage.blocking;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;

public interface BlockingWorkerOperation<M> { 
	
	public boolean execute(ChannelReader input, M source, ChannelWriter output);
	
}
