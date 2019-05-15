package com.javanut.pronghorn.stage.route;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.pipe.RawDataSchemaUtil;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class RawDataJoinerStage extends PronghornStage {

	private final Pipe<RawDataSchema> output;
	private final Pipe<RawDataSchema>[] inputs;
	private boolean[] isClosed;
    	
	//TODO: needs unit test
	public static RawDataJoinerStage newInstance(GraphManager gm, 
			 Pipe<RawDataSchema> output,
             Pipe<RawDataSchema> ... inputs) {
		return new RawDataJoinerStage(gm, output, inputs);
	}
	
	
	public RawDataJoinerStage(GraphManager gm, 
						 Pipe<RawDataSchema> output,
			             Pipe<RawDataSchema> ... inputs) {
		super(gm,inputs, output);
				
		this.inputs = inputs;
		this.output = output;
		
	}

	@Override
	public void startup() {
		isClosed = new boolean[inputs.length];
	}
	
	@Override
	public void run() {
		
		int i = inputs.length;		
		while (Pipe.hasRoomForWrite(output) && (--i >= 0)) {
			Pipe<RawDataSchema> p = inputs[i];
			if (p.hasContentToRead(p) ) {
				//accum all the data
				isClosed[i] = RawDataSchemaUtil.accumulateInputStream(p);				
				
				DataInputBlobReader<RawDataSchema> inputStream = Pipe.inputStream(p);
				
				//write what we can
				int toCopyLength = (int)Math.min(output.maxVarLen-(ChannelReader.PACKED_INT_SIZE+ChannelReader.PACKED_LONG_SIZE), inputStream.available());
				if (toCopyLength > 0) {
					int size = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
					ChannelWriter outputStream = Pipe.openOutputStream(output);
					outputStream.writePackedInt(i);
					outputStream.writePackedLong(toCopyLength);				
					inputStream.readInto(outputStream, toCopyLength);
					
					outputStream.closeLowLevelField();
					Pipe.confirmLowLevelWrite(output, size);
					Pipe.publishWrites(output);
					
					Pipe.releasePendingAsReadLock(p, toCopyLength);
				} 				
			} else {
				break;
			}
		}

		//only close when all inputs agree to close.
		int x = isClosed.length;
		while (--x >= 0) {
			if (!isClosed[x]) {
				return;
			}
		}
		if (Pipe.hasRoomForWrite(output)) {
			int s = 0;
			x = isClosed.length;
			while (--x >= 0) {
				if (Pipe.isEndOfPipe(inputs[x], Pipe.getWorkingTailPosition(inputs[x]))) {
					s++;
				}
			}
			if (s==isClosed.length) {
				Pipe.publishEOF(output);
				
				requestShutdown();
			} else {
				int size = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
				Pipe.addNullByteArray(output);
				Pipe.confirmLowLevelWrite(output, size);
				Pipe.publishWrites(output);	
				
				x = isClosed.length;
				while (--x >= 0) {
					isClosed[x] = false;//clear so we only do this once.
				}
			}
			
		}
		
		
	}

}
