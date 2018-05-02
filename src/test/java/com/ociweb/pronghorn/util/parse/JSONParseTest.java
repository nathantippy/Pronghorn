package com.ociweb.pronghorn.util.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.ociweb.json.decode.JSONDecoder;
import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.json.JSONExtractor;
import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.json.JSONType;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.StringBuilderWriter;
import com.ociweb.pronghorn.util.TrieParserReader;

public class JSONParseTest {
	
	//TODO: 1 build GL example 2 use twitter feed for unit tests. (urgent in API)

	//TODO: add first, last and collect all flags test (do later)
	
	String simple2DArrayEmptyExample = "{root: [[ "		
			+ "],["
			+ " {\"keya\":6, \"keyb\":\"six\"}  "
			+ ", {} "
			+ ", {\"keya\":7, \"keyb\":\"seven\"}  "	
			+ "]"
			+ "}";

	private final JSONDecoder simpleExtractor = new JSONDecoder()
			.begin()
				.element(JSONType.TypeString, false)//set flags for first, last, all, ordered...
					.key("root").key("keyb")
					.asField("b")
				.element(JSONType.TypeInteger, false)
					.key("root").key("keya")
					.asField("a")
			.finish();

	private final JSONDecoder simpleArrayExtractor = new JSONDecoder()
			.begin()
				.element(JSONType.TypeString, true)//set flags for first, last, all, ordered...
					.key("root").key("[]").key("keyb")
					.asField("b")
				.element(JSONType.TypeInteger, true)
					.key("root").key("[]").key("keya")
					.asField("a")
			.finish();

	private final JSONDecoder simple2DArrayExtractor = new JSONDecoder(false)
			.begin()
				.element(JSONType.TypeString, true)//set flags for first, last, all, ordered...
					.key("root").key("[]").key("[]").key("keyb")
					.asField("b")
				.element(JSONType.TypeInteger, true)
					.key("root").key("[]").key("[]").key("keya")
					.asField("a")
			.finish();

	@Test
	public void testEncodeThenDecode() {
		JSONResponse obj = new JSONResponse();
		obj.setStatusMessage(JSONResponse.StatusMessages.SUCCESS);
		StringBuilderWriter out = new StringBuilderWriter();
		obj.writeToJSON(out);

		String json = out.toString();
		assertEquals("{\"status\":200,\"message\":\"Success\",\"body\":\"\"}", json);
/*
		Pipe<RawDataSchema> targetData = parseJSON(json, JSONResponse.jsonExtractor);
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = Pipe.openInputStream(targetData);
		JSONReader reader = JSONResponse.createReader();
		obj.reset();
		obj.readFromJSON(reader, dataStream);

		assertEquals(JSONResponse.StatusMessages.SUCCESS.getStatusCode(), obj.getStatus());
		assertEquals(JSONResponse.StatusMessages.SUCCESS.getStatusMessage(), obj.getMessage());*/
	}
	
	@Ignore
	@Test //can do 150K per second
	public void loadFor2D() {
		parseJSONLoad(150_000, simple2DArrayExample, simple2DArrayExtractor);
		assert(true);
	}
	
	String simple2DArrayExample = "{root: [[ "
			+ "{\"keya\":1, \"keyb\":\"one\"}  "
			+ ", {\"keya\":2, \"keyb\":\"two\"}  "
			+ ", {\"keya\":3, \"keyb\":\"three\"}  "
			+ ", {\"keya\":4, \"keyb\":\"four\"}  "
			+ ", {\"keya\":5, \"keyb\":\"five\"}  "			
			+ "],["
			+ " {\"keya\":6, \"keyb\":\"six\"}  "
			+ ", {\"keya\":7, \"keyb\":\"seven\"}  "	
			+ "]]"
			+ "}";
	@Test
	public void simple2DArrayParseTest() {

		Pipe<RawDataSchema> targetData = parseJSON(simple2DArrayExample, simple2DArrayExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);

		long header = dataStream.readPackedLong();		
		assertEquals(0,header);
		
		assertEquals(2,      dataStream.readPackedInt());
		assertEquals(5,      dataStream.readPackedInt());
		assertEquals(2,      dataStream.readPackedInt());	
		
		assertEquals("one",  dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("two",  dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("three",   dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("four", dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("five", dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("six", dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("seven", dataStream.readUTFOfLength(dataStream.readShort()));
		
		
		assertEquals(2, dataStream.readPackedInt());
		assertEquals(5, dataStream.readPackedInt());
		assertEquals(2, dataStream.readPackedInt());
		
		assertEquals(1, dataStream.readPackedLong());
		assertEquals(2, dataStream.readPackedLong());
		assertEquals(3, dataStream.readPackedLong());
		assertEquals(4, dataStream.readPackedLong());
		assertEquals(5, dataStream.readPackedLong());
		assertEquals(6, dataStream.readPackedLong());
		assertEquals(7, dataStream.readPackedLong());
		

		
		assertEquals(0,dataStream.available());

	}
	

	private final String simpleMultipleRootExample = 
			  "{root: [ "
			+ "  {\"keya\":1, \"keyb\":\"one\"}  "
			+ ",             {\"keyb\":\"two\"}  "
			+ ", {\"keya\":3}  "
			+ ", {\"keya\":4, \"keyb\":\"four\"}  "
			+ ", {\"keya\":5, \"keyb\":\"five\"}  "			
			+ "]}"
			+ "{root: [ "
			+ "{\"keya\":1, \"keyb\":\"one\"}  "
			+ ", {\"keyb\":\"two\"}  "
			+ ", {\"keya\":3}  "
			+ ", {\"keya\":4, \"keyb\":\"four\"}  "
			+ ", {\"keya\":5, \"keyb\":\"five\"}  "			
			+ "]}";
	@Test	
	public void simpleMultipleParseTest() {

		Pipe<RawDataSchema> targetData = parseJSON(simpleMultipleRootExample, simpleArrayExtractor);
		
		while(Pipe.contentRemaining(targetData)>0) {
			
			//confirm data on the pipe is good...
			int msgIdx = Pipe.takeMsgIdx(targetData);
			ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);
	
			long header = dataStream.readPackedLong();
			assertEquals(0,header);
			
			assertEquals(5,      dataStream.readPackedInt());
			assertEquals("one",  dataStream.readUTFOfLength(dataStream.readShort()));
			assertEquals("two",  dataStream.readUTFOfLength(dataStream.readShort()));
			assertEquals(null,   dataStream.readUTFOfLength(dataStream.readShort()));
			assertEquals("four", dataStream.readUTFOfLength(dataStream.readShort()));
			assertEquals("five", dataStream.readUTFOfLength(dataStream.readShort()));
					
			assertEquals(5, dataStream.readPackedInt());
			assertEquals(1, dataStream.readPackedLong());
			assertEquals(0, dataStream.readPackedLong()); assertTrue(dataStream.wasPackedNull());
			assertEquals(3, dataStream.readPackedLong());
			assertEquals(4, dataStream.readPackedLong());
			assertEquals(5, dataStream.readPackedLong());
			
			assertEquals(0, dataStream.available());
			
			Pipe.confirmLowLevelRead(targetData, Pipe.sizeOf(targetData, msgIdx));
			Pipe.releaseReadLock(targetData);
			
		}
		
		assertEquals(0, Pipe.contentRemaining(targetData));
				
	}

	
	
	
	
	private final String simpleArrayMissingExample = "{root: [ "
			+ "{\"keya\":1, \"keyb\":\"one\"}  "
			+ ", {\"keyb\":\"two\"}  "
			+ ", {\"keya\":3}  "
			+ ", {\"keya\":4, \"keyb\":\"four\"}  "
			+ ", {\"keya\":5, \"keyb\":\"five\"}  "			
			+ "]}";
	@Test	
	public void simpleArrayMissingParseTest() {

		Pipe<RawDataSchema> targetData = parseJSON(simpleArrayMissingExample, simpleArrayExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);

		long header = dataStream.readPackedLong();		
		assertEquals(0,header);
		
		assertEquals(5,      dataStream.readPackedInt());
		assertEquals("one",  dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("two",  dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals(null,   dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("four", dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("five", dataStream.readUTFOfLength(dataStream.readShort()));
				
		assertEquals(5, dataStream.readPackedInt());
		assertEquals(1, dataStream.readPackedLong());
		assertEquals(0, dataStream.readPackedLong());
		assertTrue(dataStream.wasPackedNull());		
		assertEquals(3, dataStream.readPackedLong());
		assertEquals(4, dataStream.readPackedLong());
		assertEquals(5, dataStream.readPackedLong());
		
		assertEquals(0,dataStream.available());

	}
		
	private final String brokenObjectExample = ", {\"keya\":5, \"keyb\":\"five\"} ]}";
	
	@Test
	public void brokenObjectExampleTest() {
		
		//must not throw
		Pipe<RawDataSchema> targetData = parseJSON(brokenObjectExample, simpleArrayExtractor);
		
		assert(null!=targetData);
		
	}
	
	

	private final String simpleArrayNullExample = "{root: [ "
			+ "{\"keya\":1, \"keyb\":\"one\"}  "
			+ ", {\"keya\":null, \"keyb\":null}  "
			+ ", {\"keya\":3, \"keyb\":null}  "
			+ ", {\"keya\":4, \"keyb\":\"four\"}  "
			+ ", {\"keya\":5, \"keyb\":\"five\"}  "			
			+ "]}";
	@Test
	public void simpleArrayNullParseTest() {

		Pipe<RawDataSchema> targetData = parseJSON(simpleArrayNullExample, simpleArrayExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);

		long header = dataStream.readPackedLong();		
		assertEquals(0,header);
		
		assertEquals(5,      dataStream.readPackedInt());
		assertEquals("one",  dataStream.readUTFOfLength(dataStream.readShort())); //these are 19 bytes plus 10, 29
		assertEquals(null,   dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals(null,   dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("four", dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("five", dataStream.readUTFOfLength(dataStream.readShort()));
				
		assertEquals(5, dataStream.readPackedInt());
		assertEquals(1, dataStream.readPackedLong());
		assertEquals(0, dataStream.readPackedLong());
		assertTrue(dataStream.wasPackedNull());		
		assertEquals(3, dataStream.readPackedLong());
		assertEquals(4, dataStream.readPackedLong());
		assertEquals(5, dataStream.readPackedLong());
		
		assertEquals(0,dataStream.available());

	}
	
	private final String simpleArrayExample = "{root: [ "
			+ "{\"keya\":1, \"keyb\":\"one\"}  "
			+ ", {\"keya\":2, \"keyb\":\"two\"}  "
			+ ", {\"keya\":3, \"keyb\":\"three\"}  "
			+ ", {\"keya\":4, \"keyb\":\"four\"}  "
			+ ", {\"keya\":5, \"keyb\":\"five\"}  "			
			+ "]}";
	
	@Test
	public void simpleArrayParseTest() {
		try {

		Pipe<RawDataSchema> targetData = parseJSON(simpleArrayExample, simpleArrayExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);

		long header = dataStream.readPackedLong();		
		assertEquals(0,header);
		
		assertEquals(5,      dataStream.readPackedInt());
		assertEquals("one",  dataStream.readUTFOfLength(dataStream.readShort())); //these are 19 bytes plus 10, 29
		assertEquals("two",  dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("three",dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("four", dataStream.readUTFOfLength(dataStream.readShort()));
		assertEquals("five", dataStream.readUTFOfLength(dataStream.readShort()));
				
		assertEquals(5, dataStream.readPackedInt());
		assertEquals(1, dataStream.readPackedLong());
		assertEquals(2, dataStream.readPackedLong());
		assertEquals(3, dataStream.readPackedLong());
		assertEquals(4, dataStream.readPackedLong());
		assertEquals(5, dataStream.readPackedLong());
		
		assertEquals(0,dataStream.available());
		
		} catch (Throwable t) {
			t.printStackTrace();
			throw new AssertionError("rethrow",t);
		}
	}
	
	
	
	private final String missingAExample = "{root:{\"keyb\":\"hello\"}}";
	@Test
	public void simpleMissingATest() {
		
		Pipe<RawDataSchema> targetData = parseJSON(missingAExample, simpleExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);
		
		long header = dataStream.readPackedLong();		
		assertEquals(2,header);//the second field is null so the bit pattern is 10

		String valueB = dataStream.readUTFOfLength(dataStream.readShort());
		assertEquals("hello",valueB);

		//a is missing by design
		assertEquals(0, dataStream.available()); //this MUST be zero but it is not not sure why??
				
	}
	
	private final String missingBExample = "{root:{\"keya\":123}}";
	@Test
	public void simpleMissingBTest() {
		
		Pipe<RawDataSchema> targetData = parseJSON(missingBExample, simpleExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);
		
		long header = dataStream.readPackedLong();		
		assertEquals(1,header);

		//donot read number it is supposed to be missing.
		
		long valueA = dataStream.readPackedLong();
		assertEquals(123,valueA);
			
	}
	
	
	
	private final String nullAExample = "{root:{\"keya\":null, \"keyb\":\"hello\"}}";
	@Test
	public void simpleNullATest() {
		
		Pipe<RawDataSchema> targetData = parseJSON(nullAExample, simpleExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);
		
		long header = dataStream.readPackedLong();		
		assertEquals(0,header);

		String valueB = dataStream.readUTFOfLength(dataStream.readShort());
		assertEquals("hello",valueB);

		long valueA = dataStream.readPackedLong();
		assertEquals(0,valueA);
		
		assertTrue(dataStream.wasPackedNull());
		
	}
	
	
	private final String nullBExample = "{root:{\"keya\":123, \"keyb\":null}}";
	@Test
	public void simpleNullBTest() {
	
		try {
			Pipe<RawDataSchema> targetData = parseJSON(nullBExample, simpleExtractor);
			
			//confirm data on the pipe is good...
			Pipe.takeMsgIdx(targetData);
			ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);
			
			long header = dataStream.readPackedLong();		
			assertEquals(0,header);
	
			String valueB = dataStream.readUTFOfLength(dataStream.readShort());
			assertEquals(null,valueB);
	
			long valueA = dataStream.readPackedLong();
			assertEquals(123,valueA);
				
		} catch (Throwable t) {
			t.printStackTrace();
			throw new AssertionError("rethrow",t);
		}
	
	}
	
	
	private final String simpleExample = "{root:{\"keya\":123, \"keyb\":\"hello\"}}";
	@Test
	public void simpleParseTest() {
		
		Pipe<RawDataSchema> targetData = parseJSON(simpleExample, simpleExtractor);
		
		//confirm data on the pipe is good...
		Pipe.takeMsgIdx(targetData);
		ChannelReader dataStream = (ChannelReader)Pipe.openInputStream(targetData);
		
		long header = dataStream.readPackedLong();		
		assertEquals(0,header);

		String valueB = dataStream.readUTFOfLength(dataStream.readShort());
		assertEquals("hello",valueB);

		long valueA = dataStream.readPackedLong();
		assertEquals(123,valueA);
			
	}



	private Pipe<RawDataSchema> parseJSON(String sourceData, JSONDecoder extractor) {
		/////////////////
		//source test data.
		PipeConfig<RawDataSchema> testInputDataConfig = RawDataSchema.instance.newPipeConfig(4, 512);
		Pipe<RawDataSchema> testInputData = new Pipe<RawDataSchema>(testInputDataConfig);
		testInputData.initBuffers();
		
		int size = Pipe.addMsgIdx(testInputData, 0);
		Pipe.addUTF8(sourceData, testInputData);
		Pipe.confirmLowLevelWrite(testInputData, size);
		Pipe.publishWrites(testInputData);				
		////
		
		TrieParserReader reader = new TrieParserReader(5,true);
		
		//start consuming the data from the pipe
		int msgIdx = Pipe.takeMsgIdx(testInputData);
		TrieParserReader.parseSetup(reader ,testInputData); 

		//export data to this pipe 	
		PipeConfig<RawDataSchema> targetDataConfig = RawDataSchema.instance.newPipeConfig(4, 512);
		Pipe<RawDataSchema> targetData = new Pipe<RawDataSchema>(targetDataConfig);
		targetData.initBuffers();

		Pipe.confirmLowLevelRead(testInputData, Pipe.sizeOf(testInputData, msgIdx));
		Pipe.releaseReadLock(testInputData);

		//parse data data		
		JSONStreamParser parser = new JSONStreamParser();
		JSONStreamVisitorToChannel visitor = extractor.newJSONVisitor();
		
		do {
			parser.parse( reader,
					extractor.trieParser(), 
					visitor);
			
			/////write the captured data into the pipe
			Pipe.presumeRoomForWrite(targetData);
			int writeSize = Pipe.addMsgIdx(targetData, 0);
			DataOutputBlobWriter<RawDataSchema> stream = Pipe.openOutputStream(targetData);
			visitor.export(stream,null);		
			stream.closeLowLevelField();
			Pipe.confirmLowLevelWrite(targetData, writeSize);
			Pipe.publishWrites(targetData);
		} while (visitor.isReady() && TrieParserReader.parseHasContent(reader));
		
		
		
		return targetData;
	}

	
	private void parseJSONLoad(int i,
			                   String sourceData,
							   JSONDecoder extractor) {

		PipeConfig<RawDataSchema> targetDataConfig = RawDataSchema.instance.newPipeConfig(4, 512);
		Pipe<RawDataSchema> targetData = new Pipe<RawDataSchema>(targetDataConfig);
		targetData.initBuffers();

		TrieParserReader reader = new TrieParserReader(5,true);
		JSONStreamParser parser = new JSONStreamParser();

		PipeConfig<RawDataSchema> testInputDataConfig = RawDataSchema.instance.newPipeConfig(
															4, 512);
		Pipe<RawDataSchema> testInputData = new Pipe<RawDataSchema>(testInputDataConfig);
		testInputData.initBuffers();
		
		/////////////////
		/////////////////
		/////////////////
		
		JSONStreamVisitorToChannel visitor = extractor.newJSONVisitor();
		byte[] sourceBytes = sourceData.getBytes();
		
		while (--i>=0) {
			
			/////////////
			//write JSON data
			/////////////
			
			assertTrue("content size "+Pipe.contentRemaining(testInputData),Pipe.contentRemaining(testInputData)==0);
			int size = Pipe.addMsgIdx(testInputData, 0);
			Pipe.addByteArray(sourceBytes, testInputData);
			Pipe.confirmLowLevelWrite(testInputData, size);
			Pipe.publishWrites(testInputData);				
			////
			
			//////////
			//start consuming the data from the pipe
			//call the parser
			///////
			int msgIdx = Pipe.takeMsgIdx(testInputData);
			TrieParserReader.parseSetup(reader ,testInputData); 
	
			//parse data data		
			parser.parse( reader,
					      extractor.trieParser(), 
					      visitor);
			Pipe.confirmLowLevelRead(testInputData, Pipe.sizeOf(testInputData, msgIdx));
			Pipe.releaseReadLock(testInputData);
			
			/////write the captured data into the pipe
			int writeSize = Pipe.addMsgIdx(targetData, 0);
			DataOutputBlobWriter<RawDataSchema> stream = Pipe.openOutputStream(targetData);
			visitor.export(stream,null);
			stream.closeLowLevelField();
			Pipe.confirmLowLevelWrite(targetData, writeSize);
			Pipe.publishWrites(targetData);
		
			/////////////////////
			//read the parsed data
			////////////////////
			
			
			RawDataSchema.consume(targetData);
		}
	}
}
