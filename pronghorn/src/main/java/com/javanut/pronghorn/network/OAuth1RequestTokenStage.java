package com.javanut.pronghorn.network;

import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeWriter;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 */
public class OAuth1RequestTokenStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputs; 
    private final Pipe<ClientHTTPRequestSchema> clientRequestsPipe;
    
    private OAuth1HeaderBuilder oauth;	
    
	private static final String scheme   = "https";
	private static final int    port     = 443;		
	private static final String host     = "api.twitter.com";	
	private static final String pathRoot = "/oauth/request_token";
	private final HTTPSpecification<?, ?, ?, ?> httpSpec;
	
	private int responseId;
	
	public OAuth1RequestTokenStage(GraphManager graphManager, 
			                       Pipe<HTTPRequestSchema>[] inputs, 
			                       Pipe<ClientHTTPRequestSchema> clientRequestsPipe,
			                       int responseId,
			                       HTTPSpecification<?, ?, ?, ?> httpSpec) {
		
		super(graphManager, inputs, clientRequestsPipe);
		this.inputs = inputs;
		this.clientRequestsPipe = clientRequestsPipe;
		this.httpSpec = httpSpec;
		this.responseId = responseId;
	}

	@Override
	public void startup() {
		this.oauth = new OAuth1HeaderBuilder(port, scheme, host, pathRoot);
		
	}
	
	@Override
	public void run() {
		
		int i = inputs.length;
		while (Pipe.hasRoomForWrite(clientRequestsPipe)  && --i>=0) {
			sendRequest(inputs[i], clientRequestsPipe);
		}
		
	}

	private void sendRequest(Pipe<HTTPRequestSchema> inputPipe, 
			                 Pipe<ClientHTTPRequestSchema> outputPipe) {
				
		while (Pipe.hasContentToRead(inputPipe)) {
			int msgIdx = Pipe.takeMsgIdx(inputPipe);
	
		    long fieldChannelId = Pipe.takeLong(inputPipe);
 	        int fieldSequence = Pipe.takeInt(inputPipe);
		    int fieldVerb = Pipe.takeInt(inputPipe);
		    
		    //field params
		    DataInputBlobReader<HTTPRequestSchema> stream = Pipe.openInputStream(inputPipe);
		    
		    String consumerKey = stream.readUTF(); 
		    
//		    int status = stream.readShort();
//		    	
//            //Need to read the user ID?
//		    
//			int headerId = stream.readShort();
//			
//			while (-1 != headerId) { //end of headers will be marked with -1 value
//						
//		
//				System.out.println("first call ");
//				System.out.print(httpSpec.headers[headerId]);
//				System.out.print(" ");
//				httpSpec.writeHeader(System.out, headerId, stream);
//				System.out.println();
//							
//				
//				//read next
//				headerId = stream.readShort();
//			}
		    
		    

		    
		    System.out.println("consumer key is: "+consumerKey);
		    
		    
		    //example
//		    OAuth 
//		    oauth_consumer_key="OqEqJeafRSF11jBMStrZz", 
//		    oauth_signature="Pc%2BMLdv028fxCErFyi8KXFM%2BddU%3D", 
//		    oauth_signature_method="HMAC-SHA1", 
//		    oauth_timestamp="1300228849", 
		    //oauth_nonce="K7ny27JTpKVsTgdyLdDfmQQWVLERj2zAK5BslRsqyw", 
//		    oauth_version="1.0"
//		    oauth_callback="http%3A%2F%2Fmyapp.com%3A3005%2Ftwitter%2Fprocess_callback", 
		    		
		    //
		   // String callback = "http%3A%2F%2Fmyapp.com%3A3005%2Ftwitter%2Fprocess_callback";
		    String callback = "oob";//for pin mode
		    
		    //must set oauth calllback for every call
		    oauth.setupStep1(consumerKey,callback);

		    
		    int fieldRevision = Pipe.takeInt(inputPipe);
		    int fieldRequestContext = Pipe.takeInt(inputPipe);
		    
		    
		    int size = Pipe.addMsgIdx(outputPipe, 
		    		ClientHTTPRequestSchema.MSG_GET_200);
		    Pipe.addIntValue(1, outputPipe); //session
		    Pipe.addIntValue(port, outputPipe); //port
		    int hostId = ClientCoordinator.registerDomain(host);
		    Pipe.addIntValue(hostId, outputPipe);
		    Pipe.addLongValue(-1, outputPipe); 
		    Pipe.addIntValue(responseId, outputPipe); //destination
		    
		    Pipe.addUTF8(pathRoot, outputPipe); //path
		    //headers
			DataOutputBlobWriter<ClientHTTPRequestSchema> hStream = PipeWriter.outputStream(outputPipe);
			DataOutputBlobWriter.openField(hStream);
			oauth.addHeaders(hStream, "GET").append("\r\n");
			
			//oauth.addHeaders(System.out, "GET").append("\r\n");
			
			DataOutputBlobWriter.closeHighLevelField(hStream, ClientHTTPRequestSchema.MSG_GET_200_FIELD_HEADERS_7);
			Pipe.confirmLowLevelWrite(outputPipe, size);
			Pipe.publishWrites(outputPipe);
					 
			

		    Pipe.confirmLowLevelRead(inputPipe, Pipe.sizeOf(inputPipe, msgIdx));
		    Pipe.releaseReadLock(inputPipe);
		    
		    
		    
		    
		    
			
		}
		
		
		
		//String consumerKey = ""; //pass this consumer key to get the RequestKey
		//this.oauth.setupStep1(consumerKey);
		
		
		
	}

}
