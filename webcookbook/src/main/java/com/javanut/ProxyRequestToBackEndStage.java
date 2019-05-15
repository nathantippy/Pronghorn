package com.javanut;

import com.javanut.pronghorn.network.ClientCoordinator;
import com.javanut.pronghorn.network.ServerCoordinator;
import com.javanut.pronghorn.network.config.HTTPContentTypeDefaults;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPHeaderDefaults;
import com.javanut.pronghorn.network.config.HTTPRevisionDefaults;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerbDefaults;
import com.javanut.pronghorn.network.http.HeaderWriter;
import com.javanut.pronghorn.network.http.HeaderWriterLocal;
import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.StructuredReader;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class ProxyRequestToBackEndStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputPipes;
	private final Pipe<ConnectionData>[] connectionId;
	private final Pipe<ClientHTTPRequestSchema>[] clientRequests;
	private final String targetHost;
	private final int targetPort;
	private HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> spec;
	
	public static ProxyRequestToBackEndStage newInstance(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes, 
			Pipe<ConnectionData>[] connectionId, 
			Pipe<ClientHTTPRequestSchema>[] clientRequests,
			ServerCoordinator serverCoordinator) {
		return new ProxyRequestToBackEndStage(graphManager, inputPipes, 
				                    connectionId, clientRequests, 
				                    serverCoordinator);
	}	
	
	public ProxyRequestToBackEndStage(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes, 
			Pipe<ConnectionData>[] connectionId, 
			Pipe<ClientHTTPRequestSchema>[] clientRequests,
			ServerCoordinator serverCoordinator) {
		
		super(graphManager, inputPipes, join(clientRequests,connectionId));
		this.inputPipes = inputPipes;
		this.connectionId = connectionId;
		this.clientRequests = clientRequests;
		assert(inputPipes.length == connectionId.length);
		assert(inputPipes.length == clientRequests.length);
		this.targetHost = serverCoordinator.host();
		this.targetPort = serverCoordinator.port();
		this.spec = serverCoordinator.spec;
		
		assert(targetHost!=null);
		ClientCoordinator.registerDomain(targetHost);
	}

	@Override
	public void run() {
	
		int i = inputPipes.length;
		while (--i >= 0) {
			process(inputPipes[i], connectionId[i], clientRequests[i]);
		}
		
	}
	


	private void process(
			Pipe<HTTPRequestSchema> sourceRequest, 
			Pipe<ConnectionData> targetConnectionData, 
			Pipe<ClientHTTPRequestSchema> targetClientRequest
			) {
	
		while (Pipe.hasContentToRead(sourceRequest) && 
			   Pipe.hasRoomForWrite(targetConnectionData) &&
			   Pipe.hasRoomForWrite(targetClientRequest)
				) {
		
			int requestIdx = Pipe.takeMsgIdx(sourceRequest);

			final long connectionId = Pipe.takeLong(sourceRequest);
			final int sequenceNo = Pipe.takeInt(sourceRequest);
						
			int verb       = Pipe.takeInt(sourceRequest);
			
			DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(sourceRequest);
		
			int fieldDestination = 0; //pipe index for the response
			int fieldSession =1; //value for us to know which this belongs
			
			assert(fieldDestination>=0);
						
			int size = Pipe.addMsgIdx(targetClientRequest, ClientHTTPRequestSchema.MSG_GET_200);
			
			Pipe.addIntValue(fieldSession, targetClientRequest);
			Pipe.addIntValue(targetPort, targetClientRequest);
			Pipe.addIntValue(ClientCoordinator.registerDomain(targetHost), targetClientRequest);
			Pipe.addLongValue(-1,  targetClientRequest);
			Pipe.addIntValue(fieldDestination, targetClientRequest);
						
			StructuredReader reader = inputStream.structured();
			
			DataOutputBlobWriter<ClientHTTPRequestSchema> outputStream = Pipe.openOutputStream(targetClientRequest);			
			reader.readText(WebFields.proxyGet, outputStream);
			DataOutputBlobWriter.closeLowLevelField(outputStream);
				
			
			//open as output stream for headers..
			DataOutputBlobWriter<ClientHTTPRequestSchema> headerStream = Pipe.openOutputStream(targetClientRequest);			
			final HeaderWriter headWriter = (HeaderWriterLocal.get().target(headerStream));
			reader.visit(HTTPHeader.class, (header, hr, fId) -> {
				if (   (header != HTTPHeaderDefaults.HOST)
						&& (header != HTTPHeaderDefaults.CONNECTION)	){					
					headWriter.write((HTTPHeader)header, spec, hr);
				}
			});
			DataOutputBlobWriter.closeLowLevelField(headerStream);
			
			Pipe.confirmLowLevelWrite(targetClientRequest, size);
			Pipe.publishWrites(targetClientRequest);
			
			int revision   = Pipe.takeInt(sourceRequest);
			final int context    = Pipe.takeInt(sourceRequest);
			

			
			
			int size2 = Pipe.addMsgIdx(targetConnectionData, ConnectionData.MSG_CONNECTIONDATA_1);
			Pipe.addLongValue(connectionId, targetConnectionData);
			Pipe.addIntValue(sequenceNo, targetConnectionData);
			Pipe.addIntValue(context, targetConnectionData);
			Pipe.confirmLowLevelWrite(targetConnectionData, size2);
			Pipe.publishWrites(targetConnectionData);
			
			Pipe.confirmLowLevelRead(sourceRequest, Pipe.sizeOf(sourceRequest, requestIdx));
			Pipe.releaseReadLock(sourceRequest);
								
		}
	}
}

