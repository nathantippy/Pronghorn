package com.javanut.pronghorn.stage.blocking;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;

@Deprecated
public interface Choosable<T extends MessageSchema<T>> {

	int choose(Pipe<T> t);
	
}
