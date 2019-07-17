package com.javanut.pronghorn.network.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.network.HTTPUtilResponse;
import com.javanut.pronghorn.network.config.HTTPContentType;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPRevision;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerb;
import com.javanut.pronghorn.network.config.HTTPVerbDefaults;
import com.javanut.pronghorn.network.http.HeaderWritable;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.util.AppendableByteWriter;

/**
 * Abstraction for response payloads using UTF-8 or other text encoding.
 * See ByteArrayPayloadResponseStage to implement a custom REST responder that responds
 * only using bytes.
 *
 * Use this to define custom HTTP REST behavior.
 * @param <T> content type
 * @param <R> revisions
 * @param <V> verbs
 * @param <H> headers
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public abstract class AbstractAppendablePayloadResponseStage <   
                                T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> 
						extends PronghornStage {

	protected final HTTPSpecification<T,R,V, H> httpSpec;
	
	private final Pipe<HTTPRequestSchema>[] inputs;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final GraphManager graphManager;		
	private static final Logger logger = LoggerFactory.getLogger(AbstractAppendablePayloadResponseStage.class);
	
	private final int messageFragmentsRequired;	
	public final HTTPUtilResponse ebh = new HTTPUtilResponse();

    /**
     *
     * @param graphManager graph
     * @param inputs _in_ HTTP schema request inputs
     * @param outputs _out_ Response schema outputs
     * @param httpSpec http constants used
     * @param payloadSizeBytes payload response size
     */
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
            Pipe<HTTPRequestSchema>[] inputs, 
            Pipe<ServerResponseSchema>[] outputs,
			HTTPSpecification<T, R, V, H> httpSpec, int payloadSizeBytes) {
			super(graphManager, inputs, outputs);
			
			this.httpSpec = httpSpec;
			this.inputs = inputs;
			this.outputs = outputs;		
			this.graphManager = graphManager;			
			
			this.messageFragmentsRequired = (payloadSizeBytes/minVarLength(outputs))+2;//+2 for header and round up.
			int i = outputs.length;
			while (--i >= outputs.length) {
				if (messageFragmentsRequired* Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_TOCHANNEL_100)
						   >= outputs[i].sizeOfSlabRing) {
					throw new UnsupportedOperationException("\nMake pipe larger or lower max payload size: max message size "+payloadSizeBytes+" will not fit into pipe "+outputs[i]);
				}
			}

			assert(inputs.length == inputs.length);
			
			this.supportsBatchedPublish = false;
			this.supportsBatchedRelease = false;
			
			GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
			
	}

    /**
     *
     * @param graphManager
     * @param inputs _in_ Input request
     * @param outputs _out_ Output response
     * @param httpSpec
     * @param otherInputs _in_ Other inputs
     * @param payloadSizeBytes
     */
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
			                 Pipe<HTTPRequestSchema>[] inputs,
			                 Pipe<ServerResponseSchema>[] outputs,
							 HTTPSpecification<T, R, V, H> httpSpec,
							 Pipe<?>[] otherInputs, int payloadSizeBytes) {
		super(graphManager, join(inputs,otherInputs), outputs);
		
		this.httpSpec = httpSpec;
		this.inputs = inputs;
		this.outputs = outputs;		
		this.graphManager = graphManager;
		
		this.messageFragmentsRequired = (payloadSizeBytes/minVarLength(outputs))+1;//+1 for header
		int i = outputs.length;
		while (--i >= outputs.length) {
			if (messageFragmentsRequired > outputs[i].sizeOfSlabRing) {
				throw new UnsupportedOperationException("\nMake pipe larger or lower max payload size: max message size "+payloadSizeBytes+" will not fit into pipe "+outputs[i]);
			}
		}
		
		assert(inputs.length == inputs.length);
		
		 GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);

	}

	
	@Override
	public void run() {
		int i = this.inputs.length;
		while (--i >= 0) {			
			process(inputs[i], outputs[i]);			
		}
	}
	
	private void process(Pipe<HTTPRequestSchema> input, 
			                Pipe<ServerResponseSchema> output) {

		while (
				Pipe.hasRoomForWrite(output,
				messageFragmentsRequired * Pipe.sizeOf(output, ServerResponseSchema.MSG_TOCHANNEL_100) ) 
				&& Pipe.hasContentToRead(input)) {

			int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:

		        	long activeChannelId = Pipe.takeLong(input);
		        	int activeSequenceNo = Pipe.takeInt(input);
		        	
		        	int temp = Pipe.takeInt(input);//verb
    	    	    //int routeId = temp>>>HTTPVerb.BITS;
	    	        int fieldVerb = HTTPVerb.MASK & temp;
		        			        	
		        	DataInputBlobReader<HTTPRequestSchema> paramStream = Pipe.openInputStream(input);//param
		        	
		        	int parallelRevision = Pipe.takeInt(input);
		        	//int parallelId = parallelRevision >>> HTTPRevision.BITS;
		        	//int fieldRevision = parallelRevision & HTTPRevision.MASK;

		        	int activeFieldRequestContext = Pipe.takeInt(input);
	
				    ChannelWriter outputStream = HTTPUtilResponse.openHTTPPayload(ebh, output, activeChannelId, activeSequenceNo);
					
					boolean allWritten = payload(outputStream, graphManager, paramStream, (HTTPVerbDefaults)httpSpec.verbs[fieldVerb]); //should return error and take args?
					if (!allWritten) {
						throw new UnsupportedOperationException("Not yet implemented support for chunks");
						//TODO: can return false 404 if this is too large and we should.
							//					boolean sendResponse = true;
							//					if (!sendResponse) {
							//
							//		        		HTTPUtil.publishStatus(activeChannelId, activeSequenceNo, 404, output); 
							//		        	}
					}				
					if (!Pipe.validateVarLength(output, outputStream.length())) {
						throw new UnsupportedOperationException("dot file is too large: "+outputStream.length());
					}
					
					HeaderWritable additionalHeaderWriter = null;
					
					HTTPUtilResponse.closePayloadAndPublish(ebh, eTag(), contentType(),
						output, activeChannelId, activeSequenceNo, activeFieldRequestContext,
						outputStream, additionalHeaderWriter, 200);
		        	
		        			        	
		        	//must read context before calling this
		        	
		        	activeChannelId = -1;
				break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
		    Pipe.releaseReadLock(input);
		    
		}
	}

	//return true if this was the entire payload.
	protected abstract boolean payload(AppendableByteWriter<?> payload,
			                          GraphManager gm, 
			                          ChannelReader params, 
			                          HTTPVerbDefaults verb);
	
	protected boolean continuation(AppendableByteWriter<?> payload,
			                       GraphManager gm) {
		return false;//only called when we need to chunk
	}	
	
	public byte[] eTag() {
		return null;//override if the eTag and caching needs to be supported.
	}
	
	public abstract HTTPContentType contentType();
	
}
