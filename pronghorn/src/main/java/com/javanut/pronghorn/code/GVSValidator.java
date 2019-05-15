package com.javanut.pronghorn.code;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface GVSValidator {

	TestFailureDetails validate(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs);

	String status();

}
