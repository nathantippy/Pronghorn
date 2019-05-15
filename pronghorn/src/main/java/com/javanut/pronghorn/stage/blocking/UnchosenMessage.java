package com.javanut.pronghorn.stage.blocking;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;

@Deprecated
public interface UnchosenMessage<T extends MessageSchema<T>> {

		boolean message(Pipe<T> pipe);
}
