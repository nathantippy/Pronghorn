package com.javanut.pronghorn.code;

import java.util.Random;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class ExpectedUseGeneratorStage extends PronghornStage {

	private final Random random;
	private final GraphManager graphManager;
	private final Pipe[] inputs;
	private final Pipe[] outputs;
	private final GGSGenerator generator;

	/**
	 *
	 * @param graphManager
	 * @param inputs _in_ Input pipes
	 * @param outputs _out_ Output pipes
	 * @param random
	 * @param generator
	 */
	public ExpectedUseGeneratorStage(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs, Random random, GGSGenerator generator) {
		super(graphManager, inputs, outputs);
		this.graphManager = graphManager;
		this.inputs = inputs;
		this.outputs = outputs;
		this.random = random;		
		this.generator = generator;
	}

	@Override
	public void run() {
		
		if (!generator.generate(graphManager,inputs,outputs,random)) {
			//force hard shut down of stage under test
			GraphManager.terminateInputStages(graphManager);
			//force hard shut down of this stage
			GraphManager.setStateToShutdown(graphManager, stageId);
		}
		
	}

}
