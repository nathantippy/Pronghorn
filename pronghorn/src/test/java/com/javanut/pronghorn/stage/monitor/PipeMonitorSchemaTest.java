package com.javanut.pronghorn.stage.monitor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.javanut.pronghorn.network.schema.MQTTClientResponseSchema;
import com.javanut.pronghorn.pipe.util.build.FROMValidation;
import com.javanut.pronghorn.stage.monitor.PipeMonitorSchema;

public class PipeMonitorSchemaTest {
	
	@Test
	public void testFROMMatchesXML() {
		assertTrue(FROMValidation.checkSchema("/PipeMonitor.xml", PipeMonitorSchema.class));
	}


}
