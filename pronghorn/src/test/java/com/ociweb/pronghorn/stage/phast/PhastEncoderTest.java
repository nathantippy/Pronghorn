package com.ociweb.pronghorn.stage.phast;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.phast.*;

import static org.junit.Assert.*;


public class PhastEncoderTest {
	
	@Test
	public void testEncodeString() throws IOException{
		//create a new blob pipe to put a string on 
		Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		pipe.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(pipe);
		
		//encode a string on blob using the static method
		StringBuilder testString = new StringBuilder("This is a test");
		PhastEncoder.encodeString(writer, testString , 0, 0, false);
		
		writer.close();
		
		//check what is on the pipe
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(pipe);
		//should be -63
		int test = reader.readPackedInt();
		//the string
		String value = reader.readUTF();
		
		reader.close();
		
		String s = value.toString();
		assertTrue((test==-63) && (s.compareTo("This is a test")==0));
		
	}
	
	@Test
	public void copyIntTest() throws IOException{
		//create blob for test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//create int dictionary
		int[] intDictionary = new int[5];
		Arrays.fill(intDictionary, 0);
		intDictionary[2] = 5;
		
		//make it increment 2 values 0 and 5
		PhastEncoder.copyInt(intDictionary, writer, 0, 0, 2, 0, false);
		writer.close();
	}
	
	@Test
	public void defaultIntTest() throws IOException{
		//create a blob to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//make int array
		int[] defaultInt = new int[5];
		defaultInt[3] = 4;
		
		//should encode 16
		PhastEncoder.encodeDefaultInt(defaultInt, writer, 1, 1, 3, 16, false);
		//should encode 4
		PhastEncoder.encodeDefaultInt(defaultInt, writer, 0, 1, 3, 16, false);
		
		writer.close();
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		int test1 = reader.readPackedInt();
		//shouldnt encode anything
		reader.close();

	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////                    MASTER LONG ENCODE TEST INCLUDES ALL LONG TESTS                    //////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void encodeLongTest() throws IOException{
		//create slab to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//set up dictionaries
		long[] defaultLongDictionary = new long[5];
		defaultLongDictionary[2] = 3468;
		long[] longDictionary = new long[5];
		longDictionary[4] = 2834;
		
		
		long defaultTest = 455;
		
		//should encode: 455
		PhastEncoder.encodeLongPresent(writer, 0, 1, defaultTest, false);
		//should encode: 2834
		PhastEncoder.incrementLong(longDictionary, writer, 1, 1, 4, false);
		//should encode: 2835
		PhastEncoder.incrementLong(longDictionary, writer, 0, 1, 4, false);
		//should encode: 2835
		PhastEncoder.copyLong(longDictionary, writer, 0, 1, 4, 0, false);
		//should encode: 3468
		PhastEncoder.encodeDefaultLong(defaultLongDictionary, writer, 0, 1, 2, defaultTest, false);
		//should encode 455
		PhastEncoder.encodeDefaultLong(defaultLongDictionary, writer, 1, 1, 2, defaultTest, false);
		
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
        assertTrue(reader.readPackedLong()==455);
		assertTrue(reader.readPackedLong()==455);
		reader.close();
	}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////                    MASTER SHORT ENCODE TEST INCLUDES ALL LONG TESTS                    //////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void encodeShortTest() throws IOException{
		//create slab to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//set up dictionaries
		short[] defaultShortDictionary = new short[5];
		defaultShortDictionary[2] = 8239;
		short[] shortDictionary = new short[5];
		shortDictionary[4] = 347;
		
		
		short defaultTest = 342;
		
		//should encode: 342
		PhastEncoder.encodeShortPresent(writer, 0, 1, defaultTest, false);
		//should encode: 347
		PhastEncoder.incrementShort(shortDictionary, writer, 1, 1, 4, false);
		//should encode: 348
		PhastEncoder.incrementShort(shortDictionary, writer, 0, 1, 4, false);
		//should encode: 348
		PhastEncoder.copyShort(shortDictionary, writer, 0, 1, 4, (short)0, false);
		//should encode: 8239
		PhastEncoder.encodeDefaultShort(defaultShortDictionary, writer, 0, 1, 2, defaultTest, false);
		//should encode 342
		PhastEncoder.encodeDefaultShort(defaultShortDictionary, writer, 1, 1, 2, defaultTest, false);
		
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		assertTrue(reader.readPackedLong()==342);
		assertTrue(reader.readPackedLong()==342);
		reader.close();
	}
}
