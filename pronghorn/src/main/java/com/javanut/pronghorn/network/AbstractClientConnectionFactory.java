package com.javanut.pronghorn.network;

import java.io.IOException;

public abstract class AbstractClientConnectionFactory {

	public abstract ClientConnection newClientConnection(ClientCoordinator ccm, int port,
			int sessionId, long connectionId, int requestPipeIdx, int responsePipeIdx, int hostId, long timeoutNS, int structureId)
			throws IOException;
	
}
