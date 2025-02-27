package com.javanut.pronghorn.network;

import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.http.HTTPUtil;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * Dummy REST stage that simply returns <strong>501</strong>. Use this to quickly build yourself a web server without
 * worrying about implementation, or to test concurrent behavior.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class DummyRestStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputPipes;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final HTTPSpecification<?,?,?,?> httpSpec;
	
	public static DummyRestStage newInstance(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes,
			Pipe<ServerResponseSchema>[] outputs,
			HTTPSpecification<?,?,?,?> httpSpec) {
		return new DummyRestStage(graphManager, inputPipes, outputs, httpSpec);
	}

	/**
	 *
	 * @param graphManager
	 * @param inputPipes _in_ Input pipes containing the HTTP request
	 * @param outputs _out_ No output except 501 error.
	 * @param httpSpec
	 */
	public DummyRestStage(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes,
			Pipe<ServerResponseSchema>[] outputs,
			HTTPSpecification<?,?,?,?> httpSpec) {
		super(graphManager,inputPipes, outputs);
		this.inputPipes = inputPipes;
		this.outputs = outputs;
		this.httpSpec = httpSpec;
		
		if (inputPipes.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	}

	@Override
	public void run() {
		int i = inputPipes.length;
		while(--i>=0) {
			process(inputPipes[i], outputs[i]);
		}
	}

	private void process(Pipe<HTTPRequestSchema> input, 
			             Pipe<ServerResponseSchema> output) {
		
		while (Pipe.hasContentToRead(input)) {
			
		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:
		        	System.err.println("dummy rest request");
					long fieldChannelId = Pipe.takeLong(input);
					int fieldSequence = Pipe.takeInt(input);
					int fieldVerb = Pipe.takeInt(input);
					DataInputBlobReader<HTTPRequestSchema> data = Pipe.openInputStream(input);
					int fieldRevision = Pipe.takeInt(input);
					int fieldRequestContext = Pipe.takeInt(input);
					
					HTTPUtil.publishStatus(fieldChannelId, fieldSequence, 501, output);
					
			        Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
			        Pipe.releaseReadLock(input);
										
		            
		        break;
		        case -1:
		        	Pipe.publishEOF(output);
		        break;
		    }
		    PipeReader.releaseReadLock(input);
		}
	}

}
