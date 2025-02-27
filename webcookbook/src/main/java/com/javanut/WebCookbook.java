package com.javanut;

import java.io.File;

import com.javanut.json.JSONAccumRule;
import com.javanut.json.JSONExtractorCompleted;
import com.javanut.json.JSONType;
import com.javanut.json.decode.JSONExtractor;
import com.javanut.pronghorn.network.DummyRestStage;
import com.javanut.pronghorn.network.HTTPServerConfig;
import com.javanut.pronghorn.network.NetGraphBuilder;
import com.javanut.pronghorn.network.ServerCoordinator;
import com.javanut.pronghorn.network.ServerFactory;
import com.javanut.pronghorn.network.TLSCertificates;
import com.javanut.pronghorn.network.http.ModuleConfig;
import com.javanut.pronghorn.network.http.RouterStageConfig;
import com.javanut.pronghorn.network.module.FileReadModuleStage;
import com.javanut.pronghorn.network.module.ResourceModuleStage;
import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.network.schema.ReleaseSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeConfig;
import com.javanut.pronghorn.stage.blocking.BlockingSupportStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.stage.scheduling.StageScheduler;
import com.javanut.pronghorn.util.MainArgs;

public class WebCookbook  {

	private static ServerCoordinator serverCoordinator;
	
	public static void main(String[] args) {
		
		String home = System.getenv().get("HOME");
		String filesPath = MainArgs.getOptArg("files", "-f", args, (null==home?"~":home)+"/www");
		String host = MainArgs.getOptArg("host", "-h", args, null);
		int port = Integer.valueOf(MainArgs.getOptArg("port", "-p", args, "8080"));
				
		GraphManager gm = new GraphManager();		
		populateGraph(gm, host, port, filesPath);		
		//gm.enableTelemetry(8089);		
		StageScheduler.defaultScheduler(gm).startup();
		
	}


	private static void populateGraph(GraphManager gm, String host, int port, String filesPath) {
		
		HTTPServerConfig serverConfig = NetGraphBuilder.serverConfig(port, gm);
		
		//show all these
		serverConfig.setHost(host);
		serverConfig.setDecryptionUnitsPerTrack(2);
		serverConfig.setConcurrentChannelsPerDecryptUnit(8);
		serverConfig.setEncryptionUnitsPerTrack(2);
		serverConfig.setMaxResponseSize(1<<14);
		//serverConfig.logTraffic(); //do not log traffic when we run on build server 
		serverConfig.setMaxRequestSize(1<<10);
		serverConfig.setMaxQueueIn(10);
		serverConfig.setMaxQueueOut(10);
		
		serverConfig.useInsecureServer();//TODO: turn this off later...
		
		
		serverCoordinator = serverConfig.buildServerCoordinator();
		
		NetGraphBuilder.buildServerGraph(gm, serverCoordinator, new ServerFactory() {
		
			@Override
			public void buildServer(GraphManager gm, 
									ServerCoordinator coordinator,
									Pipe<ReleaseSchema>[] releaseAfterParse, 
									Pipe<NetPayloadSchema>[] receivedFromNet,
									Pipe<NetPayloadSchema>[] sendingToNet) {
								
				NetGraphBuilder.buildHTTPStages(gm, coordinator, buildModules(filesPath), 
										        releaseAfterParse, receivedFromNet, sendingToNet);
			}
		});
			
	}

	private static ModuleConfig buildModules(String filesPath) {
		
		return new ModuleConfig() {

			@Override
			public int moduleCount() {
				return 6;
			}

			@Override
			public Pipe<ServerResponseSchema>[] registerModule(
					int moduleInstance, GraphManager graphManager,
					RouterStageConfig routerConfig, 
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
				switch(moduleInstance) {
				
					case 0: //files served from resources
						{
						//if we like we can create one module for each input pipe or as we do here
					    //create one module to consume all the pipes and produce results.
						Pipe<ServerResponseSchema>[] response = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(2, 1<<14));
								
						ResourceModuleStage.newInstance(graphManager, 
								inputPipes, 
								response, 
								routerConfig.httpSpec(),
								"site/","reqPerSec.png");
						
						//http://10.10.10.105:8080/resource/reqPerSec.png
						//http://172.16.10.221:8080/resource/reqPerSec.png
						routerConfig.registerCompositeRoute()
						            .path("/resource/${path}") //multiple paths can be added here
						            .routeId();
						
						return response;
						}
					case 1: //files served from drive folder
						{
							
						PipeConfig<ServerResponseSchema> config
									= ServerResponseSchema.instance.newPipeConfig(32, 1<<10);
							
						Pipe<ServerResponseSchema>[] response = Pipe.buildPipes(inputPipes.length, config);
						
						int instances = inputPipes.length;
						
						Pipe<ServerResponseSchema>[] responses = new Pipe[instances];
						
						File rootPath = new File(filesPath);
										
						//creates 1 file responder for every input, we could have just built 1 and had them share
						int i = instances;
						while (--i>=0) {
							responses[i] = new Pipe<ServerResponseSchema>(config);
							FileReadModuleStage.newInstance(graphManager, inputPipes[i],
							responses[i], 
							routerConfig.httpSpec(), 
							rootPath);					
						}
							
						//http://10.10.10.105:8080/file/GLLatency2.png
						//http://172.16.10.221:8080/file/GLLatency2.png
						routerConfig.registerCompositeRoute().path("/file/${path}").routeId();
						
						return responses;
						}					
					case 2: //dummy REST call
						{
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(2, 1<<9));
							
						//NOTE: your actual REST logic replaces this stage.
						DummyRestStage.newInstance(
								graphManager, inputPipes, responses, 
								routerConfig.httpSpec()
								);
								
						routerConfig.registerCompositeRoute()
				            .path("/dummy/${textVal}") //multiple paths can be added here
				            .routeId();
						
						return responses;
						}
					case 3:	
						{
						
						int tracks = inputPipes.length;
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(
									tracks, 
									ServerResponseSchema.instance.newPipeConfig(2, 1<<9));
						
						Pipe<ConnectionData>[] connectionData = Pipe.buildPipes(
								tracks, 
								ConnectionData.instance.newPipeConfig(2, 1<<9));

						
						Pipe<ClientHTTPRequestSchema>[] clientRequests = Pipe.buildPipes(
									tracks, 
									ClientHTTPRequestSchema.instance.newPipeConfig(2, 1<<9));

						Pipe<NetResponseSchema>[] clientResponses = Pipe.buildPipes(
									tracks, 
									NetResponseSchema.instance.newPipeConfig(2, 1<<9));
						
						//TODO: turn this on later..
						TLSCertificates tlsCertificates = null;//TLSCertificates.defaultCerts;
						int connectionsInBits = 3;
						int maxPartialResponses = 1;					
						int clientRequestCount=5; 
						int clientRequestSize=200;
						
						NetGraphBuilder.buildHTTPClientGraph(graphManager, 
															clientResponses,
															clientRequests,
															maxPartialResponses, connectionsInBits,
															clientRequestCount,
															clientRequestSize,
															tlsCertificates);
												
						
						ProxyRequestToBackEndStage.newInstance(graphManager, inputPipes, 
								connectionData, clientRequests, 
								serverCoordinator);

						ProxyResponseFromBackEndStage.newInstance(graphManager, 
								                                  clientResponses, 
								                                  connectionData, 
								                                  responses,
								                                  serverCoordinator
								                                  );
								
						routerConfig.registerCompositeRoute()
				            .path("/proxy${myCall}") //multiple paths can be added here
				            .associatedObject("myCall", WebFields.proxyGet)
				            .routeId();
						
						return responses;
						}
					case 4:
						{
							
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(2, 1<<9));
						
						long timeoutNS = 10_000_000_000L;//10sec
														
						for(int i = 0; i<inputPipes.length; i++) {
							
							DBCaller[] callers = new DBCaller[8];
							int c = callers.length;
							while (--c>=0) {
								callers[c] = new DBCaller();
							}
							
							//one blocking stage for each of the tracks
							new BlockingSupportStage<HTTPRequestSchema,
							        ServerResponseSchema,ServerResponseSchema>(graphManager, 
									inputPipes[i], responses[i], responses[i], 
									timeoutNS, 
									(t)->{return ((int)(long) Pipe.peekInt(t, HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_CHANNELID_21))%inputPipes.length;}, 
									(p)-> true,
									callers); 
						}
						
						// http://172.16.10.221:8080/person/add?id=333&name=nathan
						// http://172.16.10.221:8080/person/list
						
						// http://10.10.10.105:8080/person/add?id=333&name=nathan
						// http://10.10.10.105:8080/person/list
						
						routerConfig.registerCompositeRoute()
						    .path("/person/list") //multiple paths can be added here
				            .path("/person/add?id=#{id}&name=${name}") //multiple paths can be added here
				            .defaultInteger("id", Integer.MIN_VALUE)
							.defaultText("name", "")
							.associatedObject("id", WebFields.id)
							.associatedObject("name", WebFields.name)							
				            .routeId();
						
						return responses;
						}
					case 5:
						{
							Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
									 ServerResponseSchema.instance.newPipeConfig(1<<12, 1<<9));
								
							int i = inputPipes.length;
							while (--i>=0) {
								ExampleRestStage.newInstance(
										graphManager, 
										inputPipes[i], 
										responses[i], 
										routerConfig.httpSpec()
										);
							}
				
							JSONExtractorCompleted extractor =
									new JSONExtractor()
									 .begin()
									 .stringField("name", WebFields.name)
									 .booleanField("happy", WebFields.happy)
									 .integerField("age", WebFields.age)
									 .finish();
							
							routerConfig.registerCompositeRoute(extractor)
							            .path("/hello")
							            
							         // .responseType(type) 
							         // .responseType(type,jsonObject? struct??) 
								            
							            //by default all response types
							         //   .verbGet()
							            //by default these are all routes...
							            .routeId(Routes.primary);
							
							return responses;
						}
					default:
						throw new UnsupportedOperationException();

				}				
			}			
		};
	} 
}
