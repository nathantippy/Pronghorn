package com.javanut.pronghorn.network;

import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 */
public class OAuth1AccessTokenStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputs; 
    private final Pipe<ClientHTTPRequestSchema> clientRequestsPipe;
    
    private OAuth1HeaderBuilder oauth;
    
	private static final String scheme   = "https";
	private static final int    port     = 443;		
	private static final String host     = "userstream.twitter.com";// api.twitter.com";		
	private static final String pathRoot = "/1.1/user.json";
	
	public OAuth1AccessTokenStage(GraphManager graphManager, 
			                        Pipe<HTTPRequestSchema>[] inputs,
			                        Pipe<ClientHTTPRequestSchema> clientRequestsPipe,
				                    int responseId,
				                    HTTPSpecification<?, ?, ?, ?> httpSpec) {
		
		super(graphManager, inputs, clientRequestsPipe);
		this.inputs = inputs;
		this.clientRequestsPipe = clientRequestsPipe;
	}

	@Override
	public void startup() {
		this.oauth = new OAuth1HeaderBuilder(port, scheme, host, pathRoot);
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		String consumerKey = ""; //pass this consumer key to get the RequestKey
		String consumerSecret = "";
		String token = "";
		String tokenSecret = "";
		this.oauth.setupStep2(consumerKey, consumerSecret, token, tokenSecret);
		
		
		
		
		
		
	}

}
