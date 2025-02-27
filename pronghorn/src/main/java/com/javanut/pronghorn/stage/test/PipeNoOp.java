package com.javanut.pronghorn.stage.test;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * Empty stage which does no work.  It takes an output pipe and writes nothing.
 * @param <T>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class PipeNoOp<T extends MessageSchema<T>> extends PronghornStage  {

    public static <T extends MessageSchema<T>> PipeNoOp<T> newInstance(GraphManager gm, Pipe<T> pipe) {
        return new PipeNoOp<T>(gm, pipe);
    }

    public static <T extends MessageSchema<T>> void newInstance(GraphManager gm, Pipe<T>[] pipe) {
    	int i = pipe.length;
    	while (--i>=0) {
    		new PipeNoOp<T>(gm, pipe[i]);
    	}
    }
    
	/**
	 *
	 * @param graphManager
	 * @param output _out_ Does nothing.
	 */
	protected PipeNoOp(GraphManager graphManager, Pipe<T> output) {
		super(graphManager, NONE, output);
		GraphManager.addNota(graphManager, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, this);
	}

	@Override
	public void run() {
	}

}
