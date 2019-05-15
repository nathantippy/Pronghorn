package com.javanut.pronghorn.neural;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface StageFactory<T extends MessageSchema<T>> {

	void newStage(GraphManager gm, Pipe<T>[] input, Pipe<T> output);
    void newStage(GraphManager gm, Pipe<T> input, Pipe<T>[] output);
	void newStage(GraphManager gm, Pipe<T>[] input, Pipe<T>[] output);
	
}
