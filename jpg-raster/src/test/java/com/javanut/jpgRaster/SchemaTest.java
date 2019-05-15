package com.javanut.jpgRaster;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.jpgRaster.JPGSchema;
import com.javanut.pronghorn.pipe.util.build.FROMValidation;

public class SchemaTest {
	
	@Test
	public void JPGSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/JPGSchema.xml", JPGSchema.class));
	}
	
}
