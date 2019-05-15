package com.javanut.pronghorn.network;

import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.ReleaseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface ClientResponseParserFactory {

	void buildParser(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema>[] ackReleaseForResponseParser);

}
