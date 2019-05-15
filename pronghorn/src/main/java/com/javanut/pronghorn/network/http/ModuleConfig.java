package com.javanut.pronghorn.network.http;

import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface ModuleConfig {
 
	int moduleCount(); 

	Pipe<ServerResponseSchema>[] registerModule(int moduleInstance, 
			                                    GraphManager graphManager,
			                                    RouterStageConfig routerConfig,
			                                    Pipe<HTTPRequestSchema>[] inputPipes);

}
