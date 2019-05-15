package com.javanut.pronghorn;

import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * Batches messages based on a limit.
 * @param <T> schema
 *
 * @author Nathan Tippy
 */

public class BatchingStage<T extends MessageSchema<T>> extends PronghornStage {

	private final int limit;
	private final Pipe<T> input;
	private final Pipe<T> output;

	/**
	 *
	 * @param gm graph this stage is to be part of
	 * @param pct percentage full before release
	 * @param input _in_ The pipe that will be batched
	 * @param output _out_ Pipe onto which batches will be released
	 */
	public BatchingStage(GraphManager gm, double pct, Pipe<T> input, Pipe<T> output) {
		super(gm, input, output);
	    
		//when we hit this limit we can then send all the data out
		this.limit = (int)(pct * input.sizeOfSlabRing);
	
		this.input = input;
		this.output = output;
		
		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, "cornsilk2", this);
		
	}

	private long tailCache;
	private int maxFrag;
	
	@Override
	public void startup() {
		tailCache = Pipe.tailPosition(input);
		maxFrag = FieldReferenceOffsetManager.maxFragmentSize(Pipe.from(input));
	}
	
	@Override
	public void run() {
		
		if (Pipe.hasContentToRead(input)) {	
			int remaining = limit;
			if (Pipe.hasRoomForWrite(output, remaining)) {
			
				//move all the data we can
				while ( remaining > 0 //active batch still has data
				     && (remaining>=maxFrag) //has a full fragment
					 ) {
					remaining -= Pipe.copyFragment(input, output);					
				}
			}
		}
	}

}
