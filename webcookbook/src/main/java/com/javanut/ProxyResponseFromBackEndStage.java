package com.javanut;

import com.javanut.pronghorn.network.HTTPUtilResponse;
import com.javanut.pronghorn.network.ServerCoordinator;
import com.javanut.pronghorn.network.config.HTTPContentType;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPRevision;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerb;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class ProxyResponseFromBackEndStage extends PronghornStage {

	private final Pipe<NetResponseSchema>[] clientResponses;
	private final Pipe<ConnectionData>[] connectionId;
	private final Pipe<ServerResponseSchema>[] responses;
	
	public final HTTPUtilResponse ebh = new HTTPUtilResponse();
	private final HTTPSpecification<? extends Enum<? extends HTTPContentType>, ? extends Enum<? extends HTTPRevision>, ? extends Enum<? extends HTTPVerb>, ? extends Enum<? extends HTTPHeader>> spec;
			
	public static ProxyResponseFromBackEndStage newInstance(GraphManager graphManager,
			Pipe<NetResponseSchema>[] clientResponses,
			Pipe<ConnectionData>[] connectionId,
			Pipe<ServerResponseSchema>[] responses,
			ServerCoordinator coordinator) {
		return new ProxyResponseFromBackEndStage(graphManager, clientResponses, connectionId, responses, coordinator);
	}	
	
	public ProxyResponseFromBackEndStage(GraphManager graphManager,
			Pipe<NetResponseSchema>[] clientResponses,
			Pipe<ConnectionData>[] connectionId,
			Pipe<ServerResponseSchema>[] responses, 
			ServerCoordinator coordinator) {
		super(graphManager, join(clientResponses,connectionId), responses);
		
		this.clientResponses = clientResponses;
		this.connectionId = connectionId;
		this.responses = responses;
		assert(clientResponses.length==connectionId.length);
		assert(clientResponses.length==responses.length);		
		this.spec = coordinator.spec;		
	}

	@Override
	public void run() {
		
		int i = clientResponses.length;
		while (--i >= 0) {
			process(clientResponses[i], connectionId[i], responses[i]);
		}
		
	}

	private void process(
			Pipe<NetResponseSchema> sourceResponse, 
			Pipe<ConnectionData> sourceConnectionData, 
			Pipe<ServerResponseSchema> output
			) {
			
		while ((Pipe.hasContentToRead(sourceConnectionData)) &&
		    (Pipe.hasContentToRead(sourceResponse)) &&
		    (Pipe.hasRoomForWrite(output))
		   ) {
			
			/////////////////////////
			int idx = Pipe.takeMsgIdx(sourceConnectionData);
			final long activeChannelId = Pipe.takeLong(sourceConnectionData);
			final int activeSequenceNo = Pipe.takeInt(sourceConnectionData);
			final int activeContext = Pipe.takeInt(sourceConnectionData);
			Pipe.confirmLowLevelRead(sourceConnectionData, Pipe.sizeOf(sourceConnectionData, idx));
			Pipe.releaseReadLock(sourceConnectionData);
			////////////////////////
		
			int respIdx = Pipe.takeMsgIdx(sourceResponse);
			if (respIdx == NetResponseSchema.MSG_RESPONSE_101) {
				long respChannelId = Pipe.takeLong(sourceResponse);
				int sessionId = Pipe.takeInt(sourceResponse);
				int flags2 = Pipe.takeInt(sourceResponse);
								

				ChannelWriter outputStream = HTTPUtilResponse.openHTTPPayload(ebh, output, 
						                     				    activeChannelId, 
						                     				    activeSequenceNo);
				
				DataInputBlobReader<?> inputStream = Pipe.openInputStream(sourceResponse);
				
				inputStream.structured().readPayload().readInto(outputStream, inputStream.available());
				
				//finish the header
				HTTPUtilResponse.closePayloadAndPublish(
						ebh, null, null, 
						output, activeChannelId, activeSequenceNo,
						activeContext 
				         | (flags2&ServerCoordinator.CLOSE_CONNECTION_MASK)
				         | (flags2&ServerCoordinator.END_RESPONSE_MASK)
						, outputStream, (w)->{
							
							inputStream.structured().visit(HTTPHeader.class, (header,reader,fId)->{
						    	//write all the headers back
						    	w.write(header, spec, reader);					   
						    });		    
							
						},200);
				
			} else if (respIdx == NetResponseSchema.MSG_CONTINUATION_102) {
				long channelIdx2 = Pipe.takeLong(sourceResponse);
				int flags2 = Pipe.takeInt(sourceResponse);
				
				Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100);
				Pipe.addLongValue(activeChannelId, output);
				Pipe.addIntValue(activeSequenceNo, output);
				
				DataInputBlobReader<?> inputStream = Pipe.openInputStream(sourceResponse);
				DataOutputBlobWriter<ServerResponseSchema> targetStream = Pipe.openOutputStream(output);//payload
				inputStream.structured().readPayload().readInto(targetStream, inputStream.available());
				
				Pipe.addIntValue(activeContext 
						         | (flags2&ServerCoordinator.CLOSE_CONNECTION_MASK)
						         | (flags2&ServerCoordinator.END_RESPONSE_MASK)
						         , output);
				Pipe.confirmLowLevelWrite(output);
				Pipe.publishWrites(output);
				

			} else if (respIdx == NetResponseSchema.MSG_CLOSED_10) {
				
				Pipe.skipNextFragment(sourceResponse, respIdx);
			} 	
			
			Pipe.confirmLowLevelRead(sourceResponse, Pipe.sizeOf(sourceResponse, respIdx));
			Pipe.releaseReadLock(sourceResponse);
			
			/////////////////////////
	
		
		}
		
	}

}
