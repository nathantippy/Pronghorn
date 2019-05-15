package com.javanut.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 */
public class OAuth1RequestTokenResponseStage extends PronghornStage {

	private final static Logger logger = LoggerFactory.getLogger(OAuth1RequestTokenResponseStage.class);
	private final Pipe<NetResponseSchema> input;
	private final Pipe<ServerResponseSchema>[] output;
	private final HTTPSpecification<?, ?, ?, ?> httpSpec;
	
	public OAuth1RequestTokenResponseStage(GraphManager graphManager, 
			Pipe<NetResponseSchema> input,
			Pipe<ServerResponseSchema>[] output,
			HTTPSpecification<?, ?, ?, ?> httpSpec) {
		super(graphManager, input, output);
		this.httpSpec = httpSpec;
		this.output = output;
		this.input = input;
	}

	@Override
	public void run() {
		
		while (Pipe.hasContentToRead(input)) {
		
			int msgIdx = Pipe.takeMsgIdx(input);
			
			if (NetResponseSchema.MSG_RESPONSE_101 == msgIdx) {
				
				long fieldConnectionId = Pipe.takeLong(input);
				int fieldSessionId = Pipe.takeInt(input);
				int fieldContextFlags = Pipe.takeInt(input);
				
				DataInputBlobReader<NetResponseSchema> stream = Pipe.openInputStream(input);
				
				if (0== stream.available()) {
					logger.info("no data in response, is connection closed?");
					
					Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
					Pipe.releaseReadLock(input);
					
				} else {
				
					logger.info("response len avil:"+stream.available());
					
				    int status = stream.readShort();
	    		    logger.info("status response:"+status);
				    
					int headerId = stream.readShort();
					
					while (-1 != headerId) { //end of headers will be marked with -1 value
								
						logger.info("call response ");
						
						StringBuilder temp = new StringBuilder();
						httpSpec.writeHeader(temp, headerId, stream);
						
						logger.info(httpSpec.headers[headerId]+" "+temp);
							
						
						//read next
						headerId = stream.readShort();
					}
				    		
					
					
					
					//Read examples from Twitter response Parser?
					
					//TODO: read the headers and get the request token
					//get  oauth_token
					//get  oauth_token_secret
					//get  oauth_callback_confirmed
					
					//TODO: send redirect server response
				}
				
			} else if (NetResponseSchema.MSG_CLOSED_10 == msgIdx) {
			
				Pipe.takeLong(input);
				Pipe.takeInt(input);
				DataInputBlobReader<NetResponseSchema> host = Pipe.openInputStream(input);
				int port = Pipe.takeInt(input);
				
			} else {
				//unexpected continuation??
				
				long fieldConnectionId = Pipe.takeLong(input);
				int fieldContextFlags = Pipe.takeInt(input);
				DataInputBlobReader<NetResponseSchema> stream = Pipe.openInputStream(input);
				
				
			}
				
			
			
			
		}
		
	}

}
