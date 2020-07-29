package com.javanut.json.parse;

import com.javanut.json.JSONAccumRule;
import com.javanut.json.JSONAligned;
import com.javanut.json.JSONType;
import com.javanut.json.decode.JSONExtractor;
import com.javanut.json.encode.JSONRenderer;
import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.util.AppendableByteWriter;

public class JSONResponse {
    private int status = 0;
    private final StringBuilder message = new StringBuilder();
    private final StringBuilder body = new StringBuilder();

    private static final JSONRenderer<JSONResponse> jsonRenderer = new JSONRenderer<JSONResponse>()
            .startObject()
            .integer("status", o->o.status)
            .string("message", (o,t)-> t.append(o.message))
            .string("body", (o,t)->t.append(o.body))
            .endObject();

    public enum Fields {
    	Status, Message, Body;
    }    

	private final JSONExtractor jsonExtractor = new JSONExtractor()
			.begin()
				.integerField(JSONAligned.ALLIGNED, JSONAccumRule.COLLECT,"status",Fields.Status)				
				.stringField(JSONAligned.ALLIGNED, JSONAccumRule.COLLECT,"message",Fields.Message)				
				.stringField(JSONAligned.ALLIGNED, JSONAccumRule.COLLECT,"body",Fields.Body)
			.finish();
    
    public void reset() {
        status = 0;
        message.setLength(0);
        this.message.setLength(0);
        body.setLength(0);
    }

    public void setStatusMessage(StatusMessages statusMessage) {
        this.status = statusMessage.getStatusCode();
        this.message.append(statusMessage.getStatusMessage());
    }

    public int getStatus() { return status; }

    public String getMessage() {
        return message.toString();
    }

    public String getBody() {
        return body.toString();
    }

    public void setBody(String body) {
        this.body.append(body);
    }

    public boolean readFromJSON(ChannelReader reader) {
    	
    	status = reader.structured().readInt(Fields.Status);
    	reader.structured().readText(Fields.Message, message);
    	reader.structured().readText(Fields.Body, body);
    	
        return true;
    }

    public void writeToJSON(AppendableByteWriter writer) {
        jsonRenderer.render(writer, this);
    }

    public enum StatusMessages {
        SUCCESS(200, "Success"),
        FAILURE(500, "Server Error"),
        BAD_REQUEST(400, "Bad Request");

        private final int statusCode;
        private final String statusMessage;

        StatusMessages(int statusCode, String statusMessage) {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getStatusMessage() {
            return statusMessage;
        }
    }
}
