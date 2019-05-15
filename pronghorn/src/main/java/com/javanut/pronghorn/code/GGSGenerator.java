package com.javanut.pronghorn.code;

import java.util.Random;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface GGSGenerator {

	boolean generate(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs, Random random);

}
