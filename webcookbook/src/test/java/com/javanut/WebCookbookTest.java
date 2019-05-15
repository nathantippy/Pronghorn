package com.javanut;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.javanut.WebCookbook;
import com.javanut.pronghorn.network.ClientCoordinator;
import com.javanut.pronghorn.network.NetGraphBuilder;
import com.javanut.pronghorn.network.TLSCertificates;
import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.stage.scheduling.NonThreadScheduler;
import com.javanut.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.javanut.pronghorn.util.Appendables;

public class WebCookbookTest {

	@Test
	public void makeCallsTest() {
		int fieldPort = (int) (3000 + (System.nanoTime()%12000));
		GraphManager.showThreadIdOnTelemetry = true;
		
	    ClientCoordinator.registerDomain("127.0.0.1");	
	    
		WebCookbook.main(new String[] {"-h", "127.0.0.1", "-p", String.valueOf(fieldPort)});
		
		GraphManager gm = new GraphManager();
				
		int tracks = 1;
		TLSCertificates tlsCertificates = null;//TLSCertificates.defaultCerts; 
		int connectionsInBits = 3;
		int maxPartialResponses = 1;					
		int clientRequestCount=7; 
		int clientRequestSize=2000;

		Pipe<ClientHTTPRequestSchema>[] clientRequests = Pipe.buildPipes(
				tracks, 
				ClientHTTPRequestSchema.instance.newPipeConfig(20, 1<<9));

		Pipe<NetResponseSchema>[] clientResponses = Pipe.buildPipes(
					tracks, 
					NetResponseSchema.instance.newPipeConfig(20, 1<<20));//large enough for files
		
		NetGraphBuilder.buildHTTPClientGraph(gm, 
											clientResponses,
											clientRequests,
											maxPartialResponses, connectionsInBits,
											clientRequestCount,
											clientRequestSize,
											tlsCertificates);

		//since we have no producing stage we have to init the buffer ourselves
		clientRequests[0].initBuffers();
		
		//these are the test requests
	
		ClientHTTPRequestSchema.publishGET(clientRequests[0], 
				1, //sessionId, which instance of this domain is it
				fieldPort, 
				ClientCoordinator.registerDomain("127.0.0.1"), 
				-1L,
				0, //pipe destination for the response
				"/person/add?id=333&name=nathan", 
				null);
		
		ClientHTTPRequestSchema.publishGET(clientRequests[0], 
				1, //sessionId, which instance of this domain is it
				fieldPort, 
				ClientCoordinator.registerDomain("127.0.0.1"),
				-1L,
				0, //pipe destination for the response
				"/person/add?id=444&name=scott", 
				null);
		
		//this is the last call which will have the second session id 		
		ClientHTTPRequestSchema.publishGET(clientRequests[0], 
				1, //sessionId, which instance of this domain is it
				fieldPort, 
				ClientCoordinator.registerDomain("127.0.0.1"),
				-1L,
				0, //pipe destination for the response
				"/person/list", 
				null);
		
		ClientHTTPRequestSchema.publishGET(clientRequests[0], 
				2, //sessionId, use different connection in parallel to the other requests
				fieldPort, 
				ClientCoordinator.registerDomain("127.0.0.1"),
				-1L, 
				0, //pipe destination for the response
				"/resource/reqPerSec.png", 
				null);
		
		ClientHTTPRequestSchema.publishGET(clientRequests[0], 
				3, //sessionId, use different connection in parallel to the other requests
				fieldPort, 
				ClientCoordinator.registerDomain("127.0.0.1"),
				-1L,
				0, //pipe destination for the response
				"/proxy/person/list", 
				null);
		
		ClientHTTPRequestSchema.publishGET(clientRequests[0], 
				3, //sessionId, use different connection in parallel to the other requests
				fieldPort, 
				ClientCoordinator.registerDomain("127.0.0.1"),
				-1L,
				0, //pipe destination for the response
				"/proxy/resource/reqPerSec.png", 
				null);
				
		int expectedResponseCount = 6;
		
		Pipe.publishEOF(clientRequests[0]);
		
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, clientResponses[0], Appendables.join(results, System.out));		
		
		NonThreadScheduler scheduler = new NonThreadScheduler(gm);
		
		
		scheduler.startup();
		int i = 200;
		while (--i>=0) {
			try {
				Thread.sleep(10);
				if (respCount(results)==expectedResponseCount) {
					break;//quit early
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			scheduler.run();
		}		
		scheduler.shutdown();
		
		int c = respCount(results);
		assertEquals("Expeced response count",expectedResponseCount,c);
		
		//show test response data.
		//System.err.println(results);
	}

	private int respCount(StringBuilder results) {
		int c = 0;
		int j = 0;
		while ((j=results.indexOf("Response",j+1))>=0) {
			c++;
		}
		return c;
	}


	
	
}
