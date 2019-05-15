package com.javanut.pronghorn.stage.blocking;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.pronghorn.pipe.util.build.FROMValidation;
import com.javanut.pronghorn.stage.blocking.BlockingWorkInProgressSchema;

public class BlockingSupportTest {

	
	@Test
	public void testFROMMatchesXML() {
		assertTrue(FROMValidation.checkSchema("/BlockingWorkInProgress.xml", BlockingWorkInProgressSchema.class));
	}
	
}
