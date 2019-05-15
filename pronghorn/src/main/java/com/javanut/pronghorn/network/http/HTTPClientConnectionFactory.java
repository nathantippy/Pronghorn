package com.javanut.pronghorn.network.http;

import java.io.IOException;

import javax.net.ssl.SSLEngine;

import com.javanut.pronghorn.network.AbstractClientConnectionFactory;
import com.javanut.pronghorn.network.ClientConnection;
import com.javanut.pronghorn.network.ClientCoordinator;
import com.javanut.pronghorn.network.config.HTTPHeaderDefaults;
import com.javanut.pronghorn.struct.StructRegistry;
import com.javanut.pronghorn.util.TrieParser;
import com.javanut.pronghorn.util.TrieParserReader;

public class HTTPClientConnectionFactory extends AbstractClientConnectionFactory{
	
	
	private final StructRegistry recordTypeData;

	private static final int initSize = 16;
	TrieParser[] headerParsers=new TrieParser[initSize];
	
	public HTTPClientConnectionFactory(StructRegistry recordTypeData) {
		this.recordTypeData = recordTypeData;
	}

	@Override
	public ClientConnection newClientConnection(ClientCoordinator ccm, 
											int port, int sessionId, //unique value for this connection definition
											long connectionId, int requestPipeIdx, int responsePipeIdx,
											int hostId, long timeoutNS, int structureId) throws IOException {
		
		
		
		SSLEngine engine =  ccm.isTLS ?
					ccm.engineFactory.createSSLEngine(ClientCoordinator.registeredDomain(hostId), port)
					:null;

		        
	    if (sessionId>=headerParsers.length) {
	    	//grow both
	    	headerParsers = grow(headerParsers, sessionId*2);    	
	    }
	    if (null==headerParsers[sessionId]) {
	    	//build
	    	headerParsers[sessionId] = HTTPUtil.buildHeaderParser(
		    			recordTypeData, 
		    			structureId,
		    			HTTPHeaderDefaults.CONTENT_LENGTH,
		    			HTTPHeaderDefaults.TRANSFER_ENCODING,
		    			HTTPHeaderDefaults.CONTENT_TYPE,
		    			HTTPHeaderDefaults.CONNECTION
	    			);
	    
	    }		
				
		return new HTTPClientConnection(engine, hostId, port, sessionId, requestPipeIdx, responsePipeIdx,
				                    connectionId,
				                    recordTypeData, timeoutNS, structureId,
				                    headerParsers[sessionId]);
			
	}

	private long[] grow(long[] source, int size) {
		long[] result = new long[size];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private TrieParser[] grow(TrieParser[] source, int size) {
		TrieParser[] result = new TrieParser[size];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

}
