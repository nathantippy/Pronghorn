package com.javanut.pronghorn.network.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.pronghorn.network.config.HTTPContentTypeDefaults;
import com.javanut.pronghorn.network.config.HTTPHeaderDefaults;
import com.javanut.pronghorn.network.config.HTTPRevisionDefaults;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerbDefaults;

public class HTTPSpecificationTest {

//  unable to support: json
//  unable to support: plain
//  unable to support: event-stream
//  unable to support: x-json-stream
	//application/x-json-stream
	
	@Test
	public void lookupTest() {
		
		HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> spec 
						= HTTPSpecification.defaultSpec();
		
		assertEquals(HTTPContentTypeDefaults.JSON, HTTPSpecification.lookupContentTypeByExtension(spec, "json"));
		assertEquals(HTTPContentTypeDefaults.PLAIN, HTTPSpecification.lookupContentTypeByExtension(spec, "plain"));
		assertEquals(HTTPContentTypeDefaults.EVENT_STREAM, HTTPSpecification.lookupContentTypeByExtension(spec, "event-stream"));
		

	}
	
}
