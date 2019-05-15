package com.javanut.pronghorn.network;

import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.ReleaseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface ServerFactory {

	void buildServer(GraphManager graphManager, ServerCoordinator coordinator, 
			         Pipe<ReleaseSchema>[] releaseAfterParse,
			         Pipe<NetPayloadSchema>[] receivedFromNet, 
			         Pipe<NetPayloadSchema>[] sendingToNet);

}
