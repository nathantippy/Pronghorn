package com.javanut.pronghorn.network;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.json.JSONExtractorCompleted;
import com.javanut.pronghorn.network.config.HTTPContentTypeDefaults;
import com.javanut.pronghorn.network.config.HTTPHeaderDefaults;
import com.javanut.pronghorn.network.config.HTTPRevisionDefaults;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerbDefaults;
import com.javanut.pronghorn.network.http.HTTP1xResponseParserStage;
import com.javanut.pronghorn.network.http.HTTP1xRouterStage;
import com.javanut.pronghorn.network.http.HTTPClientRequestStage;
import com.javanut.pronghorn.network.http.HTTPLogUnificationStage;
import com.javanut.pronghorn.network.http.HTTPRequestJSONExtractionStage;
import com.javanut.pronghorn.network.http.HTTPRouterStageConfig;
import com.javanut.pronghorn.network.http.ModuleConfig;
import com.javanut.pronghorn.network.http.RouterStageConfig;
import com.javanut.pronghorn.network.module.DotModuleStage;
import com.javanut.pronghorn.network.module.FileReadModuleStage;
import com.javanut.pronghorn.network.module.ResourceModuleStage;
import com.javanut.pronghorn.network.module.SummaryModuleStage;
import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPLogRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPLogResponseSchema;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.network.schema.ReleaseSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.network.schema.SocketDataSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeConfig;
import com.javanut.pronghorn.pipe.PipeConfigManager;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.PronghornStageProcessor;
import com.javanut.pronghorn.stage.file.FileBlobWriteStage;
import com.javanut.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.javanut.pronghorn.stage.scheduling.CoresUtil;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.util.IPv4Tools;
import com.javanut.pronghorn.util.TrieParserReader;
import com.javanut.pronghorn.util.math.PMath;

public class NetGraphBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(NetGraphBuilder.class);	
	
	/**
	 * This method is only for GreenLighting deep integration and should not be used
	 * unless you want to take responsibility for the handshake activity
	 */
	public static void buildHTTPClientGraph(GraphManager gm, 
			ClientCoordinator ccm, int responseQueue,
			final Pipe<NetPayloadSchema>[] requests, 
			final Pipe<NetResponseSchema>[] parsedResponse,
			int netResponseCount,
			int releaseCount, 
			int responseUnwrapCount, 
			int clientWrapperCount,
			int clientWriters) { 

		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] rawToParse,
								    Pipe<ReleaseSchema>[] ackReleaseForResponseParser) {
				
				buildHTTP1xResponseParser(gm, ccm, parsedResponse, rawToParse, ackReleaseForResponseParser);
			}
			
		};
		buildClientGraph(gm, ccm, responseQueue, requests, 
				         responseUnwrapCount, clientWrapperCount, clientWriters, 
				             releaseCount, netResponseCount, factory);
	}
	
	public static void buildClientGraph(GraphManager gm, 
			                            ClientCoordinator ccm, 
										int responseQueue,
										Pipe<NetPayloadSchema>[] requests,
										final int responseUnwrapCount,
										int clientWrapperCount,
										int clientWriters,
										int releaseCount, 
										int netResponseCount, 
										ClientResponseParserFactory parserFactory
										) {
	
		PipeConfig<ReleaseSchema> parseReleaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance, releaseCount, 0);
		
		//must be large enough for handshake plus this is the primary pipe after the socket so it must be a little larger.
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, responseQueue, ccm.receiveBufferSize); 	
		
		
		//pipe holds data as it is parsed so making it larger is helpful
		PipeConfig<NetPayloadSchema> clientHTTPResponseConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, 
				netResponseCount, 
				ccm.receiveBufferSize
				); 	
		
		///////////////////
		//add the stage under test
		////////////////////

				
		//the responding reading data is encrypted so there is not much to be tested
		//we will test after the unwrap
		//SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, socketResponse, clearResponse, false, 0);
		
		Pipe<NetPayloadSchema>[] socketResponse;
		Pipe<NetPayloadSchema>[] rawToParse;
		if (ccm.isTLS) {
			//NEED EVEN SPLIT METHOD FOR ARRAY.
			socketResponse = new Pipe[ccm.totalSessions()];
			rawToParse = new Pipe[ccm.totalSessions()];		
					
			int k = ccm.totalSessions();
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig, false);
				rawToParse[k] = new Pipe<NetPayloadSchema>(clientHTTPResponseConfig); //may be consumed by high level API one does not know.
			}
		} else {
			socketResponse = new Pipe[ccm.totalSessions()];
			rawToParse = socketResponse;		
			
			int k = ccm.totalSessions();
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientHTTPResponseConfig);//may be consumed by high level API one does not know.
			}
		}

		//do not have more parsers than cores and do not have more parsers than needed for pipe goal
		int maxParser = Math.min(CoresUtil.availableProcessors(),  rawToParse.length/ccm.pipesPerResponseParser);

		int proposed = Math.max(1, maxParser);

		while (0 != (rawToParse.length%proposed) && proposed>1) {
			proposed--;
		}
		//all pipes go into these response parsers evenly and we have <= # of cores and pipes count is reasonable
		final int responseParsers = proposed;
			
		
		
		int a = responseParsers + (ccm.isTLS ? responseUnwrapCount : 0);
		Pipe<ReleaseSchema>[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<ReleaseSchema>(parseReleaseConfig); //may be consumed by high level API one does not know.	
		}
		Pipe<ReleaseSchema>[] ackReleaseForResponseParser = Arrays.copyOfRange(acks, acks.length-responseParsers, acks.length); 
						
		ClientSocketReaderStage socketReaderStage = new ClientSocketReaderStage(gm, ccm, acks, socketResponse);
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketReader", socketReaderStage);
		ccm.processNota(gm, socketReaderStage);
			
		Pipe<NetPayloadSchema>[] hanshakePipes = buildClientUnwrap(gm, ccm, requests, responseUnwrapCount, socketResponse, rawToParse, acks);	

		buildClientWrapAndWrite(gm, ccm, requests, clientWrapperCount, clientWriters, hanshakePipes);	    

		parserFactory.buildParser(gm, ccm, rawToParse, ackReleaseForResponseParser);
	    
	}

	private static Pipe<NetPayloadSchema>[] buildClientUnwrap(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] requests,
			int responseUnwrapCount, Pipe<NetPayloadSchema>[] socketResponse, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema>[] acks) {
		Pipe<NetPayloadSchema>[] hanshakePipes = null;
		if (ccm.isTLS) {
			assert(socketResponse.length>=responseUnwrapCount) : "Can not split "+socketResponse.length+" repsonse pipes across "+responseUnwrapCount+" decrypt units";			
			
			int c = responseUnwrapCount;
			Pipe<NetPayloadSchema>[][] sr = Pipe.splitPipes(c, socketResponse);
			Pipe<NetPayloadSchema>[][] cr = Pipe.splitPipes(c, clearResponse);
			
			hanshakePipes = new Pipe[c];
			
			while (--c >= 0) {
				hanshakePipes[c] = new Pipe<NetPayloadSchema>(requests[0].config(),false); 
				SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, sr[c], cr[c], acks[c], hanshakePipes[c], false);
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			}
			
		}
		return hanshakePipes;
	}

	private static void buildClientWrapAndWrite(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] requests,
			int clientWrapperCount, int clientWriters, Pipe<NetPayloadSchema>[] hanshakePipes) {
		//////////////////////////////
		//////////////////////////////
		Pipe<NetPayloadSchema>[] wrappedClientRequests;		
		if (ccm.isTLS) {
			wrappedClientRequests = new Pipe[requests.length];	
			int j = requests.length;
			while (--j>=0) {								
				wrappedClientRequests[j] = new Pipe<NetPayloadSchema>(requests[j].config(),false);
			}
			
			int c = clientWrapperCount;			
			Pipe<NetPayloadSchema>[][] plainData = Pipe.splitPipes(c, requests);
			Pipe<NetPayloadSchema>[][] encrpData = Pipe.splitPipes(c, wrappedClientRequests);
			while (--c>=0) {			
				if (encrpData[c].length>0) {
					SSLEngineWrapStage wrapStage = new  SSLEngineWrapStage(gm, ccm, false, plainData[c], encrpData[c] );
					GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
				}
			}
			
			//change order of pipes for split later
			//interleave the handshakes.
			c = hanshakePipes.length;
			Pipe<NetPayloadSchema>[][] tPipes = Pipe.splitPipes(c, wrappedClientRequests);
			while (--c>=0) {
				tPipes[c] = PronghornStage.join(tPipes[c], hanshakePipes[c]);
			}
			wrappedClientRequests = PronghornStage.join(tPipes);
			////////////////////////////
			
		} else {
			wrappedClientRequests = requests;
		}
		//////////////////////////
		///////////////////////////
				
		Pipe<NetPayloadSchema>[][] clientRequests = Pipe.splitPipes(clientWriters, wrappedClientRequests);
		
		int i = clientWriters;
		
		while (--i>=0) {
			if (clientRequests[i].length>0) {
				ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, clientRequests[i]);
		    	GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketWriter", socketWriteStage);
		    	ccm.processNota(gm, socketWriteStage);
			}
		}
	}

	public static void buildHTTP1xResponseParser(
			GraphManager gm, ClientCoordinator ccm, 
			Pipe<NetResponseSchema>[] parsedResponse, 
			Pipe<NetPayloadSchema>[] rawToParse,
			Pipe<ReleaseSchema>[] ackRelease) {
		
		if (ackRelease.length>1) {
			if (parsedResponse.length != rawToParse.length) {
				throw new UnsupportedOperationException("pipes in and out must be the same count, found "
			              +rawToParse.length+" in vs "+parsedResponse.length+" out");
			}		
				
			int parts = ackRelease.length;
			
			///////////////////////////////////////////////////////////////
			//these splits must be done by some power of 2
			//it is key that each is power of 2 so the HTTP1xResponseParser can mask 
			///////////////////////////////////////////////////////////////
			
			final int masterLen = rawToParse.length;
			int perPipe = masterLen/parts;
			assert(masterLen%parts == 0) : "parts must go into pipe count evenly";
		    
		    final Pipe<NetPayloadSchema>[][] request = new Pipe[parts][];
		    final Pipe<NetResponseSchema>[][] response = new Pipe[parts][];
			
		    int remaining=masterLen;
		    int pos = 0;
		    for(int i=0; i<parts; i++) {
		    	int partLen = remaining>=perPipe ? perPipe : remaining;
		    	remaining -= partLen;
		    	
		    	Pipe<NetPayloadSchema>[] localReq = new Pipe[partLen];
		    	Pipe<NetResponseSchema>[] localRes = new Pipe[partLen];
		    	
		    	for(int j=0; j<partLen; j++) {
		    		localReq[j] = rawToParse[pos];
		    	    localRes[j] = parsedResponse[pos];
		    	    pos++;		    		
		    	}		    	
		    
		    	request[i] = localReq;
		    	response[i] = localRes;
		    }
		    
			int p = parts;
			while(--p >= 0) {
			    
				HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, request[p], response[p], ackRelease[p], ccm, HTTPSpecification.defaultSpec());
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
				ccm.processNota(gm, parser);
				
			}
		} else {
			HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, rawToParse, parsedResponse, ackRelease[0], ccm, HTTPSpecification.defaultSpec());
			GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
			ccm.processNota(gm, parser);
		}		
	}

	public static GraphManager buildHTTPServerGraph(final GraphManager graphManager, 
			                                        final ModuleConfig modules, 
			                                        final ServerCoordinator coordinator) {

		final ServerFactory factory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager gm, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
				
				buildHTTPStages(gm, coordinator, modules, 
						        releaseAfterParse, receivedFromNet, sendingToNet);
			}
			
		};
				
        return buildServerGraph(graphManager, coordinator, factory);
        
	}

	public static GraphManager buildServerGraph(final GraphManager graphManager,
													final ServerCoordinator coordinator,
													ServerFactory factory) {
		logger.trace("building server graph");
		final Pipe<NetPayloadSchema>[] encryptedIncomingGroup = Pipe.buildPipes(coordinator.maxConcurrentInputs,				
																				coordinator.pcmIn.getConfig(NetPayloadSchema.class));     
           
		
		long gt = computeGroupsAndTracks(coordinator.moduleParallelism(), coordinator.isTLS);
 
		int groups = (int)((gt>>32)&Integer.MAX_VALUE);
		int tracks = (int)gt&Integer.MAX_VALUE;
		
        Pipe<ReleaseSchema>[] ack = buildSocketReaderStage(graphManager, 
        		                                                         coordinator, 
        		                                                         groups, tracks,
        		                                                         encryptedIncomingGroup);       
        
        Pipe<NetPayloadSchema>[] handshakeIncomingGroup = null;
        Pipe<NetPayloadSchema>[] receivedFromNet;
        
        if (coordinator.isTLS) {
        	receivedFromNet = Pipe.buildPipes(	coordinator.maxConcurrentInputs,
        									coordinator.pcmIn.getConfig(NetPayloadSchema.class));  
        	handshakeIncomingGroup = populateGraphWithUnWrapStages(graphManager, coordinator, 
							        			coordinator.serverRequestUnwrapUnits, 
							        			coordinator.pcmIn.getConfig(NetPayloadSchema.class),
        			                      encryptedIncomingGroup, receivedFromNet, ack);
        } else {
        	receivedFromNet = encryptedIncomingGroup;
        }
		Pipe<NetPayloadSchema>[] fromOrderedContent = buildRemainderOfServerStagesWrite(graphManager, coordinator,
				handshakeIncomingGroup);
		
		//////////////////////
		/////////////////////
		ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator); 
		coordinator.processNota(graphManager, newConStage);
		////////////////////
		///////////////////

        factory.buildServer(graphManager, coordinator, 
        		            ack,
        		            receivedFromNet, 
        		            fromOrderedContent);

        return graphManager;
	}

	public static void buildHTTPStages(GraphManager graphManager, ServerCoordinator coordinator, ModuleConfig modules,
			Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
			Pipe<NetPayloadSchema>[] sendingToNet) {
		assert(sendingToNet.length >= coordinator.moduleParallelism()) : "reduce track count since we only have "+sendingToNet.length+" pipes";

		
		HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec = HTTPSpecification.defaultSpec();
		
		if (modules.moduleCount()==0) {
			throw new UnsupportedOperationException("Must be using at least 1 module to startup.");
		}
		
		//logger.info("build modules");
        Pipe<ServerResponseSchema>[][] fromModule = new Pipe[coordinator.moduleParallelism()][];       
        Pipe<HTTPRequestSchema>[][] toModules = new Pipe[coordinator.moduleParallelism()][];
                
        final HTTPRouterStageConfig routerConfig = buildModules(coordinator, graphManager,
        		 									modules, httpSpec, fromModule, toModules);
		
        //logger.info("build http stages 3");
        
		Pipe<HTTPLogRequestSchema>[] reqLog = new Pipe[coordinator.moduleParallelism()];
		
		Pipe<HTTPLogResponseSchema>[] resLog = new Pipe[coordinator.moduleParallelism()];
		
		Pipe<NetPayloadSchema>[][] perTrackFromNet 
			= Pipe.splitPipes(coordinator.moduleParallelism(), receivedFromNet);
		
		Pipe<NetPayloadSchema>[][] perTrackFromSuper 
			= Pipe.splitPipes(coordinator.moduleParallelism(), sendingToNet);
		
		buildLogging(graphManager, coordinator, reqLog, resLog);
		
		buildRouters(graphManager, coordinator, releaseAfterParse,
				fromModule, toModules, routerConfig, 
				false, reqLog, perTrackFromNet);
		
		//logger.info("build http ordering supervisors");
		buildOrderingSupers(graphManager, coordinator, fromModule, resLog, perTrackFromSuper);
	}

	public static void buildLogging(GraphManager graphManager,
			                        ServerCoordinator coordinator, 
			                        Pipe<HTTPLogRequestSchema>[] logReq,
			                        Pipe<HTTPLogResponseSchema>[] logRes) {
		
		if (coordinator.isLogFilesEnabled()) {

			LogFileConfig logFileConfig = coordinator.getLogFilesConfig();
			
			//walk the matching pipes and create monitor pipes
			int i = logReq.length;
			while (--i>=0) {
				logReq[i] = HTTPLogRequestSchema.instance.newPipe(32, 1<<12);	
			}
			
			if (logFileConfig.logResponses()) {
				int resTotal = 0;
				i = logRes.length;
				while (--i>=0) {	
					logRes[i] = HTTPLogResponseSchema.instance.newPipe(32, 1<<12);
				}		
			} else {
				//collector should not look for any of these
				logRes = new Pipe[0];
			}
		
			//write large blocks out to the file, logger unification will fill each message
			//NOTE: the file writer should do this however we do not know if the real destination
			//do not set smaller than 1<<19 1/4 meg or volume is impacted
			Pipe<RawDataSchema> out = RawDataSchema.instance.newPipe(8, 1<<20);
			
			//ConsoleJSONDumpStage.newInstance(graphManager, reqIn);
			//ConsoleJSONDumpStage.newInstance(graphManager, resIn);
			
			//PipeCleanerStage.newInstance(graphManager, reqIn);
			//PipeCleanerStage.newInstance(graphManager, resIn);
			
			new HTTPLogUnificationStage(graphManager, logReq, logRes, out);
						
			boolean append = false;
			new FileBlobWriteStage(graphManager, out, 
								   logFileConfig.maxFileSize()
					               ,append, 
					               logFileConfig.base(),
					               logFileConfig.countOfFiles()
					);
			
			//PipeCleanerStage.newInstance(graphManager, out);
			//ConsoleJSONDumpStage.newInstance(graphManager, out);			
			//ConsoleSummaryStage.newInstance(graphManager, out);
			
		}
	}

	public static void buildRouters(GraphManager graphManager, ServerCoordinator coordinator,
			Pipe<ReleaseSchema>[] releaseAfterParse, 
			Pipe<ServerResponseSchema>[][] fromModule,
			Pipe<HTTPRequestSchema>[][] toModules,
			final HTTPRouterStageConfig routerConfig,
			boolean captureAll, 
			Pipe<HTTPLogRequestSchema>[] log,
			Pipe[][] perTrackFromNet) {
		/////////////////////
		//create the routers
		/////////////////////
		
		PipeConfig<ServerResponseSchema> config = coordinator.pcmOut.getConfig(ServerResponseSchema.class);
		
		final int acksBase = releaseAfterParse.length-1;
		int parallelTrack = toModules.length; 
				
		while (--parallelTrack>=0) { 
									
			final int releaseIdx = acksBase-parallelTrack;
			assert(releaseIdx>=0);
			if (releaseIdx<0) {
				throw new UnsupportedOperationException("len "+toModules.length+" at "+parallelTrack+" base "+acksBase);
				
			}
			Pipe<HTTPRequestSchema>[] fromRouter = toModules[parallelTrack];
			int routeIdx = fromRouter.length;
			while (--routeIdx>=0) {
	
				JSONExtractorCompleted extractor = routerConfig.JSONExtractor(routeIdx);
				if (null != extractor) {
					
					////grow from module pipes to have one more error pipe
					Pipe<ServerResponseSchema> json404Pipe = new Pipe<ServerResponseSchema>(config);
					fromModule[parallelTrack] = PronghornStage.join(json404Pipe, fromModule[parallelTrack]);
			        ////////////
					
					Pipe<HTTPRequestSchema> newFromJSON = new Pipe<HTTPRequestSchema>( fromRouter[routeIdx].config() );
		
					HTTPRequestJSONExtractionStage.newInstance(
							 	graphManager, 
							 	extractor, 
							 	newFromJSON,
							 	fromRouter[routeIdx],
							 	json404Pipe
							);
					
					fromRouter[routeIdx] = newFromJSON;
									
				}
			}	
			////////////////////////////////////
			////////////////////////////////////
			
		    ////grow from module pipes to have one more error pipe
			Pipe<ServerResponseSchema> router404Pipe = new Pipe<ServerResponseSchema>(config);
			fromModule[parallelTrack] = PronghornStage.join(router404Pipe, fromModule[parallelTrack]);
			//////////////////
			
			HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(
					graphManager, 
					parallelTrack, 
					perTrackFromNet[parallelTrack], 
					fromRouter, 
					router404Pipe, 
					log[parallelTrack],
					releaseAfterParse[releaseIdx],
					routerConfig,
					coordinator,captureAll);        
			
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
			coordinator.processNota(graphManager, router);
		}
	}

	public static boolean supportReaderGroups = true;
	
	public static Pipe<ReleaseSchema>[] buildSocketReaderStage(
			final GraphManager graphManager, 
			final ServerCoordinator coordinator, 
			final int groups,
			final int tracks,
			final Pipe<NetPayloadSchema>[] encryptedIncomingGroup) {

		Pipe<ReleaseSchema>[] acks;

		int totalTracks = groups*tracks;		
		
		if (!coordinator.isTLS) {
			acks = Pipe.buildPipes(totalTracks, coordinator.pcmIn.getConfig(ReleaseSchema.class));		
		} else {
			///route count is messing up data
			acks = Pipe.buildPipes(totalTracks + coordinator.serverRequestUnwrapUnits, 
					               coordinator.pcmIn.getConfig(ReleaseSchema.class));						
		}
				
		buildSocketReaderGroups(graphManager, coordinator, encryptedIncomingGroup, acks, groups);        
        
		return acks;
	}

	public static long computeGroupsAndTracks(final int target, 
			                                 boolean isTLS) {
	
		//logger.info("compute groups from target: {} and TLS: {}",target,isTLS);
		
		if (isTLS) {
			return (1L<<32)|target; //only use 1 group for TLS, very compute heavy
		}
				
		if (target<=1 || !supportReaderGroups) {
			return (1L<<32)|target;
		}
		
		int selectedDif = Integer.MAX_VALUE;
		long selectedGroups = 1;
		long selectedTracks = target;
		
		
		int maxGroups = (target>3)?(int)Math.sqrt(target):target;
		int maxTracks = target;
		
		//logger.info("max groups:{} max tracks:{}",maxGroups,maxTracks);
				
		//do not check a single group
		int g = maxGroups+1;//must include this group
		int minGroup = (maxTracks>2)?2:1;
		while (--g >= minGroup) {
			int p = PMath.nextPrime(g);
			do {
				int proposal = p * g;
				
				int dif = Math.abs(proposal-target);
				
				if (dif<selectedDif ||
				    ((dif==selectedDif) && (proposal==target))		
					) {
					
					if (g%p != 0) {//if groups are divisible by tracks do not use.					
						selectedDif=dif;
						selectedGroups = g;
						selectedTracks = p;			
					}
					
				}
				
				p = PMath.nextPrime(p+1);
			} while (p<maxTracks);
		}
		
		//System.out.println("selected groups:"+selectedGroups+" tracks:"+selectedTracks);
	//	return ((selectedTracks*selectedGroups)<<32)|1;
		//change to long to return groups and tracks...
		return (selectedGroups<<32)|selectedTracks; //this works well on large box but not on my notebook.
		//return (selectedTracks<<32)|selectedGroups; //NOTE: This works on AWS test for 25% faster now that we have 7 instead of 4
		

	}

	private static void buildSocketReaderGroups(final GraphManager graphManager, final ServerCoordinator coordinator,
			final Pipe<NetPayloadSchema>[] encryptedIncomingGroup, Pipe<ReleaseSchema>[] acks, int countOfSocketReaders) {
		Pipe<NetPayloadSchema>[][] in  = Pipe.splitPipes(countOfSocketReaders, encryptedIncomingGroup);
		Pipe<ReleaseSchema>[][] out = Pipe.splitPipes(countOfSocketReaders, acks);
		
		long gt = computeGroupsAndTracks(coordinator.moduleParallelism(), coordinator.isTLS);		 
		int groups = (int)((gt>>32)&Integer.MAX_VALUE);//same as count of SocketReaders
		int tracks = (int)gt&Integer.MAX_VALUE; //tracks is count of HTTP1xRouters
		int pipes = in[0].length/tracks; //is count of pipes together going to each parser
		
		int partsPerPipe = 4;//TODO: must be computed from the number of requests etc...
		
		boolean orig = false;//tracks<=1;
		
		if (countOfSocketReaders<1) {
			throw new UnsupportedOperationException();
		}
		
		for(int x=0; x<countOfSocketReaders; x++) {
					
			if (orig) {
				ServerSocketReaderStage readerStage = new ServerSocketReaderStage(graphManager, out[(countOfSocketReaders-x)-1], in[x], coordinator);
				coordinator.processNota(graphManager, readerStage);
			} else {
				
				int varLen = PronghornStage.maxVarLength(encryptedIncomingGroup)<<3; //needed to buffer many incoming requests.
				
				Pipe<SocketDataSchema>[] localPipe = null;
				
				int routers = tracks;
				if (tracks<1) {
					throw new UnsupportedOperationException();
				}
					
				localPipe = Pipe.buildPipes(routers, 
						SocketDataSchema.instance.newPipeConfig(
								pipes*partsPerPipe,
								varLen
								)
						);
				
				//this test did not provide any speed boost so it was turned off, 
				//     it may be helpfull again in the future for direct device integration..
				boolean offHeapBlobs = false;
				if (offHeapBlobs) {
					int i = localPipe.length;
					while(--i>=0) {
						//NOTE: must use with-XX:+UseConcMarkSweepGC because other GC gets lost!!					
						localPipe[i].moveBlobOffHeap();					
					}
				}
				
				///
				//TODO: swap in a test instance to generate fast test data.
				///
				logger.trace("ServerSocketBulkReaderStage begin new "+x);
				ServerSocketBulkReaderStage readerStage = ServerSocketBulkReaderStage.newInstance(graphManager, localPipe, coordinator);
				logger.trace("ServerSocketBulkReaderStage end new "+x);
				coordinator.processNota(graphManager, readerStage);
				Pipe<ReleaseSchema>[] localAck = out[(countOfSocketReaders-x)-1];
				Pipe<NetPayloadSchema>[] localPayload = in[x];				
				
				Pipe<ReleaseSchema>[][] localOut = Pipe.splitPipes(routers, localAck);
				Pipe<NetPayloadSchema>[][] localIn = Pipe.splitPipes(routers, localPayload);
				
				int r = routers;
				while (--r>=0) {
					
					ServerSocketBulkRouterStage routerStage = ServerSocketBulkRouterStage.newInstance(graphManager, localPipe[r], 
																									localOut[(routers-r)-1], 
																									localIn[r],							
																									coordinator);
					coordinator.processNota(graphManager, routerStage);
				}
				logger.trace("ServerSocketBulkReaderStage end consumers "+x);
				
			}
			
			
			

		}
	}

	public static Pipe<NetPayloadSchema>[] buildRemainderOfServerStagesWrite(final GraphManager graphManager,
			ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] handshakeIncomingGroup) {
		Pipe<NetPayloadSchema>[] fromOrderedContent = new Pipe[
		                       coordinator.serverResponseWrapUnitsAndOutputs 
		                       * coordinator.serverPipesPerOutputEngine];

		Pipe<NetPayloadSchema>[] toWiterPipes = buildSSLWrapersAsNeeded(graphManager, coordinator, 
				                                                       handshakeIncomingGroup,
				                                                       fromOrderedContent);
                    
        buildSocketWriters(graphManager, coordinator, coordinator.serverSocketWriters, toWiterPipes);
		return fromOrderedContent;
	}

	private static Pipe<NetPayloadSchema>[] buildSSLWrapersAsNeeded(final GraphManager graphManager,
			ServerCoordinator coordinator,
			Pipe<NetPayloadSchema>[] handshakeIncomingGroup,
			Pipe<NetPayloadSchema>[] fromOrderedContent) {
		
		
		int requestUnwrapUnits = coordinator.serverRequestUnwrapUnits;		
		int y = coordinator.serverPipesPerOutputEngine;
		int z = coordinator.serverResponseWrapUnitsAndOutputs;
		Pipe<NetPayloadSchema>[] toWiterPipes = null;
		PipeConfig<NetPayloadSchema> fromOrderedConfig = coordinator.pcmOut.getConfig(NetPayloadSchema.class);
		
		if (coordinator.isTLS) {
		    
		    toWiterPipes = new Pipe[(z*y) + requestUnwrapUnits ]; //extras for handshakes if needed
		    
		    int toWriterPos = 0;
		    int fromSuperPos = 0;
		    
		    int remHanshakePipes = requestUnwrapUnits;
		    
		    while (--z>=0) {           
		    	
		    	//as possible we must mix up the pipes to ensure handshakes go to different writers.
		        if (--remHanshakePipes>=0) {
		        	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[remHanshakePipes]; //handshakes go directly to the socketWriterStage
		        }
		    	
		    	//
		    	int w = y;
		        Pipe<NetPayloadSchema>[] toWrapperPipes = new Pipe[w];
		        Pipe<NetPayloadSchema>[] fromWrapperPipes = new Pipe[w];            
		        
		        while (--w>=0) {	
		        	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromOrderedConfig,false);
		        	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromOrderedConfig,false); 
		        	toWiterPipes[toWriterPos++] = fromWrapperPipes[w];
		        	fromOrderedContent[fromSuperPos++] = toWrapperPipes[w]; 
		        }
		        
		        boolean isServer = true;
		        
				SSLEngineWrapStage wrapStage = new SSLEngineWrapStage(graphManager, coordinator,
		        		                                             isServer, toWrapperPipes, fromWrapperPipes);
		        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
		        coordinator.processNota(graphManager, wrapStage);
		    }
		    
		    //finish up any remaining handshakes
		    while (--remHanshakePipes>=0) {
		    	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[remHanshakePipes]; //handshakes go directly to the socketWriterStage
		    }
		    
		    
		} else {
		
			int i = fromOrderedContent.length;
			while (-- i>= 0) {
				fromOrderedContent[i] = new Pipe<NetPayloadSchema>(fromOrderedConfig,false);            		
			}
			toWiterPipes = fromOrderedContent;      	
		
		}
		return toWiterPipes;
	}

	public static void buildOrderingSupers(GraphManager graphManager, ServerCoordinator coordinator,
			Pipe<ServerResponseSchema>[][] fromModule, Pipe<HTTPLogResponseSchema>[] log,
			Pipe<NetPayloadSchema>[][] perTrackFromSuper) {
		
		int track = fromModule.length;
		while (--track>=0) {
		
			OrderSupervisorStage wrapSuper = new OrderSupervisorStage(graphManager, 
									                    fromModule[track], 
									                    log[track],
									                    perTrackFromSuper[track], 
									                    coordinator);//ensure order   
	
			coordinator.processNota(graphManager, wrapSuper);
		
		}
	}

	private static void buildSocketWriters(GraphManager graphManager, ServerCoordinator coordinator, 
											int socketWriters, Pipe<NetPayloadSchema>[] toWiterPipes) {
		///////////////
		//all the writer stages
		///////////////
		//TODO: special mode for testing builds custom writer object for testing.
		//      Reader will produce N messages
		//      Consumer will accept N messages
		//      we have MORE consumers than readers so we also need, 
		
		Pipe[][] req = Pipe.splitPipes(socketWriters, toWiterPipes);	
		int w = socketWriters;
		while (--w>=0) {
			
			ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, coordinator, req[w]); //pump bytes out
		    GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketWriter", writerStage);
		   	coordinator.processNota(graphManager, writerStage);
		}
	}

	public static HTTPRouterStageConfig buildModules(ServerCoordinator coordinator, GraphManager graphManager, ModuleConfig modules,
			HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec,
			Pipe<ServerResponseSchema>[][] fromModule,
			Pipe<HTTPRequestSchema>[][] toModules) {
		
		PipeConfig<HTTPRequestSchema> routerToModuleConfig = coordinator.pcmIn.getConfig(HTTPRequestSchema.class);
		
		final int trackCount = coordinator.moduleParallelism();

		final HTTPRouterStageConfig routerConfig = new HTTPRouterStageConfig(httpSpec, coordinator.connectionStruct()); 

		for(int r=0; r<trackCount; r++) {
			toModules[r] = new Pipe[modules.moduleCount()];
		}
		  
		//create each module
		for(int routeId=0; routeId<modules.moduleCount(); routeId++) { 
			
			Pipe<HTTPRequestSchema>[] routesTemp = new Pipe[trackCount];
			for(int trackId=0; trackId<trackCount; trackId++) {
				//TODO: change to use.. newHTTPRequestPipe
				//TODO: this should be false but the DOT telemetry is still using the high level API...
				routesTemp[trackId] = toModules[trackId][routeId] =  new Pipe<HTTPRequestSchema>(routerToModuleConfig);//,false);
								
			}
			//each module can unify of split across routers
			Pipe<ServerResponseSchema>[] outputPipes = modules.registerModule(
					                routeId, graphManager, routerConfig, routesTemp);
				
			
			assert(validateNoNulls(outputPipes));
		    
		    for(int r=0; r<trackCount; r++) {
		    	//accumulate all the from pipes for a given router group
		    	fromModule[r] = PronghornStage.join(fromModule[r], outputPipes[r]);
		    }
		    
		}
		
		return routerConfig;
	}

	private static boolean validateNoNulls(Pipe<ServerResponseSchema>[] outputPipes) {
		
		int i = outputPipes.length;
		while (--i>=0) {
			if (outputPipes[i]==null) {
				throw new NullPointerException("null discovered in output pipe at index "+i);
			}
			
		}
		return true;
	}

	
	
	public static Pipe<NetPayloadSchema>[] populateGraphWithUnWrapStages(GraphManager graphManager, ServerCoordinator coordinator,
			int requestUnwrapUnits, PipeConfig<NetPayloadSchema> handshakeDataConfig, Pipe[] encryptedIncomingGroup,
			Pipe[] planIncomingGroup, Pipe[] acks) {
		Pipe<NetPayloadSchema>[] handshakeIncomingGroup = new Pipe[requestUnwrapUnits];
		            	
		assert(acks.length >= requestUnwrapUnits);
		int c = requestUnwrapUnits;
		Pipe[][] in = Pipe.splitPipes(c, encryptedIncomingGroup);
		Pipe[][] out = Pipe.splitPipes(c, planIncomingGroup);
		
		while (--c>=0) {
			handshakeIncomingGroup[c] = new Pipe(handshakeDataConfig);
			SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, in[c], out[c], acks[c], handshakeIncomingGroup[c], true);
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			coordinator.processNota(graphManager, unwrapStage);
		}
		
		return handshakeIncomingGroup;
	}

	public static String bindHost(String bindHost) {
		
		
		TrieParserReader reader = new TrieParserReader(true);
		int token =  null==bindHost?-1:(int)reader.query(IPv4Tools.addressParser, bindHost);
		
		if ((null==bindHost || token>=0) && token<4) {
			boolean noIPV6 = true;//TODO: we really do need to add ipv6 support.
			List<InetAddress> addrList = NetGraphBuilder.homeAddresses(noIPV6);
			bindHost = IPv4Tools.patternMatchHost(reader, token, addrList);
		}
		return bindHost;
	}

	public static List<InetAddress> homeAddresses(boolean noIPV6) {
		List<InetAddress> addrList = new ArrayList<InetAddress>();
		try {
			Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();			
			while (networkInterfaces.hasMoreElements()) {
				NetworkInterface ifc = networkInterfaces.nextElement();
				try {
					if(ifc.isUp()) {						
						Enumeration<InetAddress> addrs = ifc.getInetAddresses();
						while (addrs.hasMoreElements()) {
							InetAddress addr = addrs.nextElement();						
							byte[] addrBytes = addr.getAddress();
							if (noIPV6) {								
								if (16 == addrBytes.length) {
									continue;
								}							
							}							
							if (addrBytes.length==4) {
								if (addrBytes[0]==127 && addrBytes[1]==0 && addrBytes[2]==0 && addrBytes[3]==1) {
									continue;
								}								
							}
							addrList.add(addr);
						}						
					}
				} catch (SocketException e) {
					//ignore
				}
			}			
		} catch (SocketException e1) {
			//ignore.
		}
		
		Comparator<? super InetAddress> comp = new Comparator<InetAddress>() {
			@Override
			public int compare(InetAddress o1, InetAddress o2) {
				return Integer.compare(o2.getAddress()[0], o2.getAddress()[0]);
			} //decending addresses			
		};
		addrList.sort(comp);
		return addrList;
	}
	

	public static void telemetryServerSetup(TLSCertificates tlsCertificates, String bindHost, int port,
			                                GraphManager gm, int baseRate) {

		///////////////
		final int serverRate = baseRate; //the server here runs slower than the base scan

		final ModuleConfig modules = buildTelemetryModuleConfig(serverRate);
		boolean isTLS = tlsCertificates != null;
		int maxConnectionBits = 8;
		int tracks = 1;
		
		//NOTE!!  if we do not have enough decypt or input units for the number of calls from 
		//        the browser then we can hang due to the way the Bulk reader, router work.
		int concurrentChannelsPerDecryptUnit = 24; //need large number for new requests
		int concurrentChannelsPerEncryptUnit = 4; //this will use a lot of memory if increased
				
		 //for cookies sent in
		
		
		HTTPServerConfig c = NetGraphBuilder.serverConfig(port, gm);
		if (!isTLS) {
			c.useInsecureServer();
		}
		c.setEncryptionUnitsPerTrack(2);
		c.setConcurrentChannelsPerEncryptUnit(concurrentChannelsPerEncryptUnit);
		c.setDecryptionUnitsPerTrack(1);
		c.setConcurrentChannelsPerDecryptUnit(concurrentChannelsPerDecryptUnit);
		c.setMaxConnectionBits(maxConnectionBits);
		
		c.setMaxRequestSize(1<<14);//NOTE: this is a little bigger in case of cookies.		
		c.setMaxResponseSize(1<<23);//NOTE: needs to be large enough for telemetry responses...
		
		((HTTPServerConfigImpl)c).setTracks(tracks);
		((HTTPServerConfigImpl)c).finalizeDeclareConnections();
		
		HTTPServerConfigImpl r = ((HTTPServerConfigImpl)c);

		int fromSocketBlocks = 16;
		int fromSocketBuffer = 1<<12;
		
		r.pcmOut.ensureSize(ServerResponseSchema.class, 4, 512);
		
		final ServerPipesConfig serverConfig = new ServerPipesConfig(
						null,//we do not need to log the telemetry traffic, so null...
						r.isTLS(),
						r.getMaxConnectionBits(),
						1, //only 1 track for telemetry
						r.getEncryptionUnitsPerTrack(),
						r.getConcurrentChannelsPerEncryptUnit(),
						r.getDecryptionUnitsPerTrack(),
						r.getConcurrentChannelsPerDecryptUnit(),				
						//one message might be broken into this many parts
						fromSocketBlocks, 
						r.getMaxRequestSize(),
						r.getMaxResponseSize(),
						2, //incoming telemetry requests in Queue, keep small
						2, //outgoing telemetry responses, keep small.
						r.pcmIn,
						r.pcmOut);
	
		ServerConnectionStruct scs = new ServerConnectionStruct(gm.recordTypeData);
		ServerCoordinator serverCoord = new ServerCoordinator(tlsCertificates,
				        bindHost, port, scs,								
				        false, "Telemetry Server","", serverConfig);

		
		
		serverCoord.setStageNotaProcessor(new PronghornStageProcessor() {
			//force all these to be hidden as part of the monitoring system
			@Override
			public void process(GraphManager gm, PronghornStage stage) {
				
				int divisor = 1;
				int multiplier = 1;
				
					
				if (stage instanceof ServerNewConnectionStage 
				 || stage instanceof ServerSocketReaderStage) {
					multiplier = 2; //rarely check since this is telemetry
				} else {
					
					int inC = GraphManager.getInputPipeCount(gm, stage.stageId);
					if (inC>1) {
						//if we join N sources all with the same schema.
							
						Pipe<?> basePipe = GraphManager.getInputPipe(gm, stage.stageId, 1);
						
						boolean allPipesAreForSameSchema = true;
						int x = inC+1;
						while(--x > 1) {
							allPipesAreForSameSchema |=
							Pipe.isForSameSchema(basePipe, 
									             (Pipe<?>) GraphManager.getInputPipe(gm, stage.stageId, x));
						}
						if (allPipesAreForSameSchema 
							&& (!(stage instanceof HTTP1xRouterStage))
							&& (!(stage instanceof OrderSupervisorStage))
								) {
							divisor = 1<<(int)(Math.rint(Math.log(inC)/Math.log(2)));
						}
						
					}
					
					
					
				}
				
				
			    //server must be very responsive so it has its own low rate.
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, (multiplier*serverRate)/divisor, 
						stage);

				//TODO: also use this to set the RATE and elminate the extra argument passed down....
				stage.setNotaFlag(PronghornStage.FLAG_MONITOR);
			}
		});
		
		final ServerFactory factory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager gm, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
	
				NetGraphBuilder.buildHTTPStages(gm, coordinator, modules, 
						        releaseAfterParse, receivedFromNet, sendingToNet);
			}			
		};
		
		NetGraphBuilder.buildServerGraph(gm, serverCoord, factory);
	}

	private static ModuleConfig buildTelemetryModuleConfig(final long rate) {
		
		//TODO: the resource server can not span fragments, but all must be in one block.
		//      as a result pipe var sizes must be large for now..
		
		ModuleConfig config = new ModuleConfig(){
			PipeMonitorCollectorStage monitor;
	
			private final String[] routes = new String[] {
					 "/${path}"			
					,"/graph.dot"
					,"/summary.json"
					,"/openapi.json"
//					,"/dataView?pipeId=#{pipeId}"
//					,"/histogram/pipeFull?pipeId=#{pipeId}"
//					,"/histogram/stageElapsed?stageId=#{stageId}"
//					,"/WS1/example" //server side websocket example
							
			};
			
			public CharSequence getPathRoute(int a) {
				return routes[a];
			}
	
			@Override
			public int moduleCount() {
				return routes.length;
			}

			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager,
					RouterStageConfig routerConfig,
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
				//the file server is stateless therefore we can build 1 instance for every input pipe
				int instances = inputPipes.length;

				Pipe<ServerResponseSchema>[] staticFileOutputs = new Pipe[instances];
				
					PronghornStage activeStage = null;
					switch (a) {
						case 0:
							
						activeStage = ResourceModuleStage.newInstance(graphManager, 
								inputPipes, 
								staticFileOutputs = Pipe.buildPipes(instances, 
										 ServerResponseSchema.instance.newPipeConfig(2, 1<<22)), 
								(HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTPRouterStageConfig)routerConfig).httpSpec,
								"telemetry/","index.html");						
						break;

						case 1:
						    if (null==monitor) {	
								monitor = PipeMonitorCollectorStage.attach(graphManager);	
						    }
							activeStage = DotModuleStage.newInstance(graphManager, monitor,
									inputPipes, 
									staticFileOutputs = Pipe.buildPipes(instances, 
										 ServerResponseSchema.instance.newPipeConfig(2, 1<<26)), 
									((HTTPRouterStageConfig)routerConfig).httpSpec);
						break;
						case 2:
						    if (null==monitor) {	
								monitor = PipeMonitorCollectorStage.attach(graphManager);	
						    }
						    int maxSummarySize = 1<<14;
							activeStage = SummaryModuleStage.newInstance(graphManager, monitor,
									inputPipes, 
									staticFileOutputs = Pipe.buildPipes(instances, 
											           ServerResponseSchema.instance.newPipeConfig(2, maxSummarySize)), 
									((HTTPRouterStageConfig)routerConfig).httpSpec);
							break;
						case 3:
												
							//look up the router config defined in the graph
							PronghornStage[] stage = GraphManager.allStagesByType(graphManager, HTTP1xRouterStage.class);
							
							byte[] json;
							if (stage.length>0) {
								HTTP1xRouterStage serverRouter = (HTTP1xRouterStage)stage[0];
								json = serverRouter.routerConfig().jsonOpenAPIBytes(graphManager);
								
							//TODO: must lookup the response in the graph as well.
							
							} else {
								json = new byte[0];
							}
							
							activeStage = new FixedRestStage(graphManager, 
									                          inputPipes, 
									                          staticFileOutputs = Pipe.buildPipes(instances, 
																           ServerResponseSchema.instance.newPipeConfig(2, 1<<21)), 
									                          HTTPContentTypeDefaults.JSON.getBytes(), json);
							
							break;
//						case 4:
//						//TODO: replace this code with the actual pipe full histogram
//							activeStage = new DummyRestStage(graphManager, 
//			                          inputPipes, 
//			                          staticFileOutputs = Pipe.buildPipes(instances, 
//									           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//			                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							break;
//						case 5:
//						//TODO: replace this code with the actual stage elapsed histogram
//							activeStage = new DummyRestStage(graphManager, 
//			                          inputPipes, 
//			                          staticFileOutputs = Pipe.buildPipes(instances, 
//									           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//			                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							break;
//						case 6:
//						//TODO: replace this code with the actual pipe full histogram
//							
//							activeStage = new UpgradeToWebSocketStage(graphManager, 
//			                          inputPipes, 
//			                          staticFileOutputs = Pipe.buildPipes(instances, 
//									           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//			                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							
//							break;
							default:
														
							throw new RuntimeException("unknown idx "+a);
					}
					
					if (null!=activeStage) {
						GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, activeStage);
						activeStage.setNotaFlag(PronghornStage.FLAG_MONITOR);						
					}
					
				if (a==5) {
					
//				             ,HTTPHeaderDefaults.ORIGIN.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_ACCEPT.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_KEY.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_PROTOCOL.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_VERSION.rootBytes()
//				             ,HTTPHeaderDefaults.UPGRADE.rootBytes()
//				             ,HTTPHeaderDefaults.CONNECTION.rootBytes()

					routerConfig.registerCompositeRoute().path( getPathRoute(a));

				} else {
					if (a==0) {
						//this header enables the resource module stage to reply with compressed responses.
						routerConfig.registerCompositeRoute(HTTPHeaderDefaults.ACCEPT_ENCODING).path( getPathRoute(a));
					} else {
						routerConfig.registerCompositeRoute().path( getPathRoute(a));
						//no headers
					}
				}
				return staticFileOutputs;			
			}
		};
		return config;
	}

	/**
	 * Build HTTP client subgraph.  This is the easiest method to set up the client calls since many default values are already set.
	 * 
	 * @param gm target graph where this will be added
	 * @param httpResponsePipe http responses 
	 * @param httpRequestsPipe http requests
	 */	
	public static void buildHTTPClientGraph(GraphManager gm,
			  int maxPartialResponses,
			  Pipe<NetResponseSchema>[] httpResponsePipe,
			  Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe) {		
		
		int connectionsInBits = 7;		
		int clientRequestCount = 4;
		int clientRequestSize = SSLUtil.MinTLSBlock;
		final TLSCertificates tlsCertificates = TLSCerts.define();

		buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
				
	}

	public static void buildSimpleClientGraph(GraphManager gm, ClientCoordinator ccm,
											  ClientResponseParserFactory factory, 
											  Pipe<NetPayloadSchema>[] clientRequests) {
		int clientWriters = 1;				
		int responseUnwrapCount = 1;
		int clientWrapperCount = 1;
		int responseQueue = 20;
		int releaseCount = 2048;
		int netResponseCount = 64; //reader to response parse
		int netResponseBlob = 1<<19;

		buildClientGraph(gm, ccm, responseQueue, clientRequests, responseUnwrapCount, clientWrapperCount, clientWriters,
				         releaseCount, netResponseCount, factory);
	}
	
	/**
	 * This is the method you want for making HTTP calls from the client side
	 * @param gm
	 * @param httpResponsePipe
	 * @param requestsPipe
	 * @param maxPartialResponses
	 * @param connectionsInBits
	 * @param clientRequestCount
	 * @param clientRequestSize
	 * @param tlsCertificates
	 */
	public static void buildHTTPClientGraph(GraphManager gm, 
			final Pipe<NetResponseSchema>[] httpResponsePipe, Pipe<ClientHTTPRequestSchema>[] requestsPipe,
			int maxPartialResponses, int connectionsInBits, int clientRequestCount, int clientRequestSize,
			TLSCertificates tlsCertificates) {
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, tlsCertificates, gm.recordTypeData);
				
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema>[] ackReleaseForResponseParser) {
				
				buildHTTP1xResponseParser(gm, ccm, httpResponsePipe, clearResponse, ackReleaseForResponseParser);
			}			
		};

		Pipe<NetPayloadSchema>[] clientRequests = Pipe.buildPipes(requestsPipe.length, NetPayloadSchema.instance.<NetPayloadSchema>newPipeConfig(clientRequestCount,clientRequestSize));
				
		buildSimpleClientGraph(gm, ccm, factory, clientRequests);
		
		new HTTPClientRequestStage(gm, ccm, requestsPipe, clientRequests);
	}

	
	public static HTTPServerConfig serverConfig(int port, GraphManager gm) {
		return new HTTPServerConfigImpl(port, new PipeConfigManager(), new PipeConfigManager(), gm.recordTypeData);				
	}
	
	public static ModuleConfig simpleFileServer(final String pathRoot, final int messagesToOrderingSuper,
			final int messageSizeToOrderingSuper) {
		//using the basic no-fills API
		ModuleConfig config = new ModuleConfig() { 
		
		    //this is the cache for the files, so larger is better plus making it longer helps a lot but not sure why.
		    final PipeConfig<ServerResponseSchema> fileServerOutgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 
		    		         messagesToOrderingSuper, messageSizeToOrderingSuper);//from module to  supervisor
	
			@Override
			public int moduleCount() {
				return 1;
			}
	
			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager, RouterStageConfig routerConfig, 
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
	
					//the file server is stateless therefore we can build 1 instance for every input pipe
					int instances = inputPipes.length;
					
					Pipe<ServerResponseSchema>[] staticFileOutputs = new Pipe[instances];
					
					int i = instances;
					while (--i>=0) {
						staticFileOutputs[i] = new Pipe<ServerResponseSchema>(fileServerOutgoingDataConfig);
						FileReadModuleStage.newInstance(graphManager, inputPipes[i], staticFileOutputs[i], (HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTPRouterStageConfig)routerConfig).httpSpec, new File(pathRoot));					
					}
						
					routerConfig.registerCompositeRoute().path("/${path}");
					//no headers requested
					
				return staticFileOutputs;
			}        
		 	
		 };
		return config;
	}
	
}
