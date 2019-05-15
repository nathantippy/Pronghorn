package com.javanut;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.ConnectionData;
import com.javanut.pronghorn.pipe.util.build.FROMValidation;

public class SchemaTest {

	@Test
    public void messageClientNetResponseSchemaFROMTest() {
    	
        assertTrue(FROMValidation.checkSchema("/ConnectionDataSchema.xml", ConnectionData.class));
    }
	
}
