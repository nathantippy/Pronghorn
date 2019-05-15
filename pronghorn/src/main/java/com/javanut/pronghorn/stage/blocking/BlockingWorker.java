package com.javanut.pronghorn.stage.blocking;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;

public interface BlockingWorker {//move to PH??

	void doWork(Pipe<RawDataSchema> input, Pipe<RawDataSchema> output);

}
