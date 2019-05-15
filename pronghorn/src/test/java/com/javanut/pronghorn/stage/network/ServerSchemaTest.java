package com.javanut.pronghorn.stage.network;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.pronghorn.network.schema.AlertNoticeSchema;
import com.javanut.pronghorn.network.schema.HTTPLogRequestSchema;
import com.javanut.pronghorn.network.schema.HTTPLogResponseSchema;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.NetPayloadSchema;
import com.javanut.pronghorn.network.schema.ServerConnectionSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.util.build.FROMValidation;

public class ServerSchemaTest {


    @Test
    public void testServerResponseSchemaFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/serverResponse.xml", ServerResponseSchema.class));
    }

    @Test
    public void testServerConnectFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/serverConnect.xml", ServerConnectionSchema.class));
    }

    @Test
    public void testHTTPRequestFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/HTTPRequest.xml", HTTPRequestSchema.class));
    }

    @Test
    public void testHTTPLogRequestFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/HTTPLogRequest.xml", HTTPLogRequestSchema.class));
    }   
    
    @Test
    public void testHTTPLogResponseFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/HTTPLogResponse.xml", HTTPLogResponseSchema.class));
    }
    
    @Test
    public void testAlertNoticeFROMMatchesXML() {
        assertTrue(FROMValidation.checkSchema("/AlertNotice.xml", AlertNoticeSchema.class));
    }
    
}
