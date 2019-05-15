package com.javanut;

import com.javanut.json.encode.JSONRenderer;
import com.javanut.pronghorn.network.config.HTTPContentType;
import com.javanut.pronghorn.network.config.HTTPContentTypeDefaults;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPRevision;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerb;
import com.javanut.pronghorn.network.config.HTTPVerbDefaults;
import com.javanut.pronghorn.network.module.AbstractAppendablePayloadResponseStage;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.StructuredReader;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.util.AppendableByteWriter;

public class ExampleRestStage<      T extends Enum<T> & HTTPContentType,
									R extends Enum<R> & HTTPRevision,
									V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> extends AbstractAppendablePayloadResponseStage<T,R,V,H> {

    private static final byte[] HEY = "Hey, ".getBytes();
	private static final byte[] BANG = "!".getBytes();
	
	private static final JSONRenderer<StructuredReader> jsonRenderer = new JSONRenderer<StructuredReader>()
            .startObject()
            .string("message", (reader,target) ->  {target.write(HEY); reader.readText(WebFields.name, target).write(BANG);} )
            .bool("happy", reader -> !reader.readBoolean(WebFields.happy))
            .integer("age", reader -> reader.readInt(WebFields.age) * 2)
            .endObject();
	
	public static ExampleRestStage newInstance(GraphManager graphManager, 
			Pipe<HTTPRequestSchema> inputPipes,
			Pipe<ServerResponseSchema> outputPipe, 
			HTTPSpecification httpSpec) {
		return new ExampleRestStage(graphManager, inputPipes, outputPipe, httpSpec);
	}
	
	public ExampleRestStage(GraphManager graphManager, 
			Pipe<HTTPRequestSchema> inputPipes,
			Pipe<ServerResponseSchema> outputPipes, 
			HTTPSpecification httpSpec) {
		
		super(graphManager, join(inputPipes), join(outputPipes), httpSpec, 1<<8);

	}

	@Override
	public HTTPContentType contentType() {
		return HTTPContentTypeDefaults.JSON;
	}

	@Override
	protected boolean payload(AppendableByteWriter<?> payload, GraphManager gm, 
			                 ChannelReader params, HTTPVerbDefaults verb) {	
		jsonRenderer.render(payload, params.structured());
		return true;
	}

}
