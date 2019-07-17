package com.javanut.pronghorn;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeWriter;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * An example for a producer.
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class ExampleProducerStage extends PronghornStage {

	private final Pipe<RawDataSchema> output;
	
	protected ExampleProducerStage(GraphManager graphManager, Pipe<RawDataSchema> output) {
		super(graphManager, NONE, output);
		this.output = output;
		GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 100_000_000, this);
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
		
	}

	@Override
	public void run() {
		if (PipeWriter.tryWriteFragment(output, RawDataSchema.MSG_CHUNKEDSTREAM_1)) {
			PipeWriter.writeASCII(output, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, "test");
			PipeWriter.publishWrites(output);
		}
	}

	
	
}
