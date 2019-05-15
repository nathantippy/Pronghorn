package com.javanut.pronghorn.stage.network.schema;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ConnectionStateSchema;
import com.javanut.pronghorn.network.schema.MQTTClientRequestSchema;
import com.javanut.pronghorn.network.schema.MQTTClientResponseSchema;
import com.javanut.pronghorn.network.schema.MQTTClientToServerSchema;
import com.javanut.pronghorn.network.schema.MQTTClientToServerSchemaAck;
import com.javanut.pronghorn.network.schema.MQTTIdRangeControllerSchema;
import com.javanut.pronghorn.network.schema.MQTTIdRangeSchema;
import com.javanut.pronghorn.network.schema.MQTTServerToClientSchema;
import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.network.schema.ReleaseSchema;
import com.javanut.pronghorn.network.schema.SocketDataSchema;
import com.javanut.pronghorn.network.schema.TwitterEventSchema;
import com.javanut.pronghorn.network.schema.TwitterStreamControlSchema;
import com.javanut.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidationTest {

    @Test
    public void messageSocketDataSchemaFROMTest() {    	
        assertTrue(FROMValidation.checkSchema("/SocketData.xml", SocketDataSchema.class));
    }
	
    @Test
    public void messageNetPayloadSchemaFROMTest() {    	
        assertTrue(FROMValidation.checkSchema("/NetPayload.xml", NetPayloadSchema.class));
    }

    @Test
    public void messageNetParseAckSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/Release.xml", ReleaseSchema.class));
    }
	
    @Test
    public void messageNetResponseSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/NetResponse.xml", NetResponseSchema.class));
    }
	    
    @Test
    public void messageNetRequestSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/ClientHTTPRequest.xml", ClientHTTPRequestSchema.class));
    }
    
    @Test
    public void messageConnectionStateSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/ConnectionState.xml", ConnectionStateSchema.class));
    }
    
    @Test
    public void messageMQTTIdRangeSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTIdRanges.xml", MQTTIdRangeSchema.class));
    }
    
    @Test
    public void messageMQTTIdRangeControllerSchemaFROMTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTIdControlRanges.xml", MQTTIdRangeControllerSchema.class));
    }
    
    @Test
    public void messageMQTTClientToServerTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientToServer.xml", MQTTClientToServerSchema.class));
    }
    
    @Test
    public void messageMQTTClientToServerAckTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientToServerAck.xml", MQTTClientToServerSchemaAck.class));
    }
    
    @Test
    public void messageMQTTServerToClientTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTServerToClient.xml", MQTTServerToClientSchema.class));
    }
    
    @Test
    public void messageMQTTClientRequestTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientRequest.xml", MQTTClientRequestSchema.class));
    }
    
    @Test
    public void messageMQTTClientResponseTest() {
        assertTrue(FROMValidation.checkSchema("/MQTTClientResponse.xml", MQTTClientResponseSchema.class));
    }
    
    
    @Test
    public void testEventsFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/TwitterEvent.xml", TwitterEventSchema.class));
    }

    @Test
    public void testTwitterUserStreamControlSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/TwitterUserStreamControl.xml", TwitterStreamControlSchema.class));
    }
}
