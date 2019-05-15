package com.javanut.pronghorn.network.http;

import com.javanut.json.JSONExtractorCompleted;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface RouterStageConfig {

	public int headerId(byte[] bytes);

	public HTTPSpecification httpSpec();
	
	public CompositeRoute registerCompositeRoute(HTTPHeader ... headers);

	public CompositeRoute registerCompositeRoute(JSONExtractorCompleted extractor, HTTPHeader ... headers);

	public byte[] jsonOpenAPIBytes(GraphManager graphManager);
	
}
