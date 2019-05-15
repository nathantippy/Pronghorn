package com.javanut.logtelemetry;

import com.javanut.pronghorn.network.HTTPServerConfig;
import com.javanut.pronghorn.network.NetGraphBuilder;
import com.javanut.pronghorn.network.ServerCoordinator;
import com.javanut.pronghorn.network.ServerFactory;
import com.javanut.pronghorn.network.http.ModuleConfig;
import com.javanut.pronghorn.network.http.RouterStageConfig;
import com.javanut.pronghorn.network.module.ResourceModuleStage;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.ReleaseSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.file.FileBlobReadStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.stage.scheduling.StageScheduler;
import com.javanut.pronghorn.util.MainArgs;

public class LogTelemetry  {

	private static ServerCoordinator serverCoordinator;
	// Create a new GraphManager. The GraphManager is essential and keeps track of stages (nodes) and pipes (edges).
	final GraphManager gm = new GraphManager();

	/**
	 * Main entry point that creates an instance of log-telemetry and starts it.
	 * @param args Arguments for starting main
	 */
	public static void main(String[] args) {

	    // Get the file path from the arguments
		String inputFilePath = MainArgs.getOptArg("fileName", "-f", args, "./greenlightning.log");

		// Create a new log-telemetry instance, put the output stream on the system log
		LogTelemetry program = new LogTelemetry(inputFilePath);

		program.startup();
		
		// 1. load file and all its data into an array
		// 2. static file hosting of telemetry files
		// 3. new stage returning each block in rotation
		
		

	}

	/**
	 * Constructor for log-telemetry
	 * @param inputFilePath The input path for the file to be logged
	 * @param port The port on which telemetry will run
	 * @param out An appendable in which the specified file gets written to
	 */
	public LogTelemetry (String logFilePath) {

		// Add edges and pipes
		populateGraph(gm, "127.0.0.1", 8099, logFilePath);
		

		// Turning on the telemetry web page is as simple as adding this line
		// It will be accessible on the most public IP it can find by default
		gm.enableTelemetry("127.0.0.1",8098);

	}

	/**
	 * Use the default scheduler with the passed in GraphManager to start
	 */
	void startup() {

		StageScheduler.defaultScheduler(gm).startup();

	}



	private static void populateGraph(GraphManager gm, String host, int port, String logFilePath) {
		
		HTTPServerConfig serverConfig = NetGraphBuilder.serverConfig(port, gm);
		
		//show all these
		serverConfig.setHost(host);
		serverConfig.setDecryptionUnitsPerTrack(4);
		serverConfig.setEncryptionUnitsPerTrack(4);
		serverConfig.setMaxResponseSize(1<<24); //TODO: match the module logic
		//serverConfig.logTraffic(); //do not log traffic when we run on build server 
		serverConfig.setMaxRequestSize(1<<11);
		serverConfig.setMaxQueueIn(32);
		serverConfig.setMaxQueueOut(4);
		
		serverConfig.useInsecureServer();//TODO: turn this off later...
		
		
		final Pipe<RawDataSchema> output = RawDataSchema.instance.newPipe(64, 1<<15);//TODO: if too small we hang...
		FileBlobReadStage.newInstance(gm, output, logFilePath, false);
		
		
		
		serverCoordinator = serverConfig.buildServerCoordinator();
		
		NetGraphBuilder.buildServerGraph(gm, serverCoordinator, new ServerFactory() {
		
			@Override
			public void buildServer(GraphManager gm, 
									ServerCoordinator coordinator,
									Pipe<ReleaseSchema>[] releaseAfterParse, 
									Pipe<NetPayloadSchema>[] receivedFromNet,
									Pipe<NetPayloadSchema>[] sendingToNet) {
								
				NetGraphBuilder.buildHTTPStages(gm, coordinator, buildModules(output), 
										        releaseAfterParse, receivedFromNet, sendingToNet);
			}
		});
			
	}

	private static ModuleConfig buildModules(final Pipe<RawDataSchema> logFile) {
		
		return new ModuleConfig() {

			@Override
			public int moduleCount() {
				return 2;
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
								 ServerResponseSchema.instance.newPipeConfig(4, 1<<22));
								
						ResourceModuleStage.newInstance(graphManager, 
								inputPipes, 
								response, 
								routerConfig.httpSpec(),
								"telemetry/",
								"index.html"); 
					
						routerConfig.registerCompositeRoute()
						            .path("/${path}")
						            .routeId();
						
						return response;
						}	
					case 1: 
						{
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(4, 1<<24));
							
						new LogTelemetryRestStage(graphManager, inputPipes, responses, logFile);
								
						routerConfig.registerCompositeRoute()
				            .path("/graph.dot") //multiple paths can be added here
				            .routeId();
						
						return responses;
						}

					default:
						throw new UnsupportedOperationException();

				}				
			}			
		};
	} 
	
}
