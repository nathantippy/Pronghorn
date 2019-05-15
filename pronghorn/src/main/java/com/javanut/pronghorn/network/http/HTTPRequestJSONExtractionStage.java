package com.javanut.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.json.JSONExtractorCompleted;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.StructuredReader;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.struct.StructRegistry;
import com.javanut.pronghorn.util.TrieParserReader;
import com.javanut.pronghorn.util.TrieParserReaderLocal;
import com.javanut.pronghorn.util.parse.JSONStreamParser;
import com.javanut.pronghorn.util.parse.JSONStreamVisitorToChannel;

/**
 * Using a JSONExtractor, takes a HTTP request with JSON and turns it into
 * a ServerResponseSchema onto a ServerResponseSchema pipe.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class HTTPRequestJSONExtractionStage extends PronghornStage {

	private final JSONExtractorCompleted extractor;
	
	private final Pipe<HTTPRequestSchema> input;
	private final Pipe<HTTPRequestSchema> output;
	private final Pipe<ServerResponseSchema> err;
	
		
	private JSONStreamParser parser;
	private JSONStreamVisitorToChannel visitor;

	
	public static final Logger logger = LoggerFactory.getLogger(HTTPRequestJSONExtractionStage.class);

	
	public static HTTPRequestJSONExtractionStage newInstance(GraphManager graphManager, 
			JSONExtractorCompleted extractor,  Pipe<HTTPRequestSchema> input,
			Pipe<HTTPRequestSchema> output,
			Pipe<ServerResponseSchema> err) {
		return new HTTPRequestJSONExtractionStage(graphManager, extractor, input, output,err);
	}
	
	/**
	 *
	 * @param graphManager
	 * @param extractor
	 * @param input _in_ The HTTP request containing JSON.
	 * @param output _out_ The HTTP response.
	 * @param err _out_ Contains ServerResponseSchema if error occurred
	 */
	public HTTPRequestJSONExtractionStage(GraphManager graphManager, 
											JSONExtractorCompleted extractor,  Pipe<HTTPRequestSchema> input,
											Pipe<HTTPRequestSchema> output,
											Pipe<ServerResponseSchema> err) {
		
		super(graphManager, input, join(output, err));
		this.extractor = extractor;
		this.input = input;
		this.output = output;
		this.err = err;

		
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		
		//boolean highVolume = false;
		//if (highVolume) {
		//	GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
		//}
	}

	@Override
	public void startup() {

		parser = new JSONStreamParser();
		visitor = extractor.newJSONVisitor();
			
	}
	

	
	@Override
	public void run() {
		
		final TrieParserReader reader = TrieParserReaderLocal.get();
				
		Pipe<HTTPRequestSchema> localInput = input;
		Pipe<HTTPRequestSchema> localOutput = output;
		
		while (Pipe.hasContentToRead(localInput) && Pipe.hasRoomForWrite(localOutput) ) {
			
		    int msgIdx = Pipe.takeMsgIdx(localInput);
		   
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:
		        {
		        	long channelId = Pipe.takeLong(localInput);
		        	int sequenceNum = Pipe.takeInt(localInput);
		        	int verb = Pipe.takeInt(localInput);      	
		        	
		        	DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(localInput);
		        	DataOutputBlobWriter<HTTPRequestSchema> outputStream = Pipe.openOutputStream(localOutput);

		        	/////////////////
		        	//copies params and headers.
		        	int payloadOffset = inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION);		   
		        	assert(payloadOffset>=0) : "offset must be positive but was "+payloadOffset;
					inputStream.readInto(outputStream, payloadOffset);
		        	assert(DataInputBlobReader.absolutePosition(inputStream)>=0) : "position must not be negative";
		        	
		        	int avail = inputStream.available();
		        	
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	DataInputBlobReader.setupParser(inputStream, reader);
		        	
		        	boolean debugJSON = false;
		        	if (debugJSON) {
		        		reader.debugAsUTF8(reader, System.out, avail);
		        		System.out.println();
		        	}
		        	
		    		parser.parse(reader, extractor.trieParser(), visitor);
		    		
		    		//if (TrieParserReader.parseHasContent(reader)) {
		    		//	logger.info("calls detected with {} bytes after JSON.",TrieParserReader.parseHasContentLength(reader));
		    		//}
		    		
		    		if (!visitor.isReady() && visitor.isValid()) {
		    			
		    			final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			Pipe.addLongValue(channelId, localOutput); //channel
		    			Pipe.addIntValue(sequenceNum, localOutput); //sequence
		    			Pipe.addIntValue(verb, localOutput); //verb
		    			
		    			//moves the index data as is and must happen before JSON updates index
		    			inputStream.readFromEndInto(outputStream);
		    			//parser is not "ready for data" and requires export to be called
		    			//this export will populate the index positions for the JSON fields

		    			visitor.export(outputStream, extractor.getIndexPositions());
		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());
		    			
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
		    			
		    			Pipe.addIntValue(Pipe.takeInt(localInput), localOutput); //revision
		    			Pipe.addIntValue(Pipe.takeInt(localInput), localOutput); //context
		    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    					    			
		    		} else {
		    			localOutput.closeBlobFieldWrite();	    			
		    			
		    			
		    			if (!visitor.isValid()) {
		    				logger.warn("\nJSON was parsed but contained invalid field values.");		    			
		    			}
		    			
		    			if (Pipe.hasRoomForWrite(err)) {
		    				HTTPUtil.publishStatus(channelId, sequenceNum, 400, err);
		    			} else {
		    				//some errors are already sent, do not worry about these.
		    				logger.trace("too many errors to send while parsing JSON");
		    			}
		    			
		    			visitor.clear();//rest for next JSON
		    					    			
		    			Pipe.takeInt(localInput);// consume revision before release
		    			Pipe.takeInt(localInput);// consume context before release
		    			
		    		}
		    		
		    		
		        }	
				break;
		        case HTTPRequestSchema.MSG_WEBSOCKETFRAME_100:
		        {	
		        	long channelId = Pipe.takeLong(localInput);
		        	int sequenceNum = Pipe.takeInt(localInput);
		        	int finOpp = Pipe.takeInt(localInput);
		        	int maskVal = Pipe.takeInt(localInput);
		        	
		        	DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(localInput);
					
		        	assert(inputStream.isStructured()) : "Structured stream is required for JSON";

		        	int payloadOffset = inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION);
		    		        			        	
		        	int size = Pipe.addMsgIdx(localOutput, msgIdx);
		        	Pipe.addLongValue(channelId, localOutput); //channel
		        	Pipe.addIntValue(sequenceNum, localOutput); //sequence
		        	Pipe.addIntValue(finOpp, localOutput); //FinOpp
		        	Pipe.addIntValue(maskVal, localOutput); //Mask
		        	DataOutputBlobWriter<HTTPRequestSchema> outputStream = Pipe.openOutputStream(localOutput);
		        	inputStream.readInto(outputStream, payloadOffset);//copies params and headers.
				    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	reader.parseSetup(inputStream);
		        			    		
		    		//for each block? as it goes for post? TODO: do later...
		    		parser.parse(reader, extractor.trieParser(), visitor);
		        			    
		    		if (visitor.isReady()) {
		    			//needs more data.
		    			
		    		} else {	
		    			
		    		}
		    		
		    		//TODO: may need multiple of these for streaming..
		    		visitor.export(outputStream, extractor.getIndexPositions());		
		    		DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());
	    			
		    		
		    		//moves the index data as is
		        	inputStream.readFromEndInto(outputStream); //TODO: only needed on first block
		        					    
				    Pipe.confirmLowLevelWrite(localOutput,size);
				    Pipe.publishWrites(localOutput);
		        }
		        break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(localInput, Pipe.sizeOf(HTTPRequestSchema.instance, msgIdx));
		    Pipe.releaseReadLock(localInput);
		    			
			
		}
						
	}

}
