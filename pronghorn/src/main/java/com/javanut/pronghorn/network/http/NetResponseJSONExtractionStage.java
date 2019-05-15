package com.javanut.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.json.JSONExtractorCompleted;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
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
public class NetResponseJSONExtractionStage extends PronghornStage {
	
	private final JSONExtractorCompleted extractor;
	
	private final Pipe<NetResponseSchema> input;
	private final Pipe<NetResponseSchema> output;

	private JSONStreamParser parser;
	private JSONStreamVisitorToChannel visitor;

	private final StructRegistry typeData;

	public static final Logger logger = LoggerFactory.getLogger(HTTPRequestJSONExtractionStage.class);

	/**
	 *
	 * @param graphManager
	 * @param extractor
	 * @param input _in_ The HTTP request containing JSON.
	 * @param output _out_ The HTTP response.
	 */
	public NetResponseJSONExtractionStage(  GraphManager graphManager, 
											JSONExtractorCompleted extractor, 
											Pipe<NetResponseSchema> input,
											Pipe<NetResponseSchema> output) {
		
		super(graphManager, input, output);
		this.extractor = extractor;
		this.input = input;
		this.output = output;
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
		this.typeData = graphManager.recordTypeData;
		
		
	}

	@Override
	public void startup() {

		parser = new JSONStreamParser();
		visitor = extractor.newJSONVisitor();
			
	}
		
	@Override
	public void run() {
		
		final TrieParserReader reader = TrieParserReaderLocal.get();
				
		Pipe<NetResponseSchema> localInput = input;
		Pipe<NetResponseSchema> localOutput = output;
		
		while (Pipe.hasContentToRead(localInput) && Pipe.hasRoomForWrite(localOutput) ) {
			
		    int msgIdx = Pipe.takeMsgIdx(localInput);
		   
		    switch(msgIdx) {
		    
		    	case NetResponseSchema.MSG_RESPONSE_101:
		        {
		        	//hold to pass forward
		        	long conId = Pipe.takeLong(localInput);
		        	int userSessionId = Pipe.takeInt(localInput);
		        	int contextFlags = Pipe.takeInt(localInput);
		        	DataInputBlobReader<NetResponseSchema> inputStream = Pipe.openInputStream(localInput); //payload
	       			        	
		        	//copies params and headers.
		        	DataOutputBlobWriter<NetResponseSchema> outputStream = Pipe.openOutputStream(localOutput);
		        	inputStream.readInto(outputStream, inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION));
		    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	DataInputBlobReader.setupParser(inputStream, reader);
		    		parser.parse(reader, extractor.trieParser(), visitor);
		    		
		    		//if (TrieParserReader.parseHasContent(reader)) {
		    		//	logger.info("calls detected with {} bytes after JSON.",TrieParserReader.parseHasContentLength(reader));
		    		//}
		    		
		    		if (!visitor.isReady()  && visitor.isValid()) {
		    			
		    			final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			
		    			Pipe.addLongValue(conId, localOutput);
		    			Pipe.addIntValue(userSessionId, localOutput);
		    			Pipe.addIntValue(contextFlags, localOutput);
		    			
		    			//moves the index data as is and must happen before JSON updates index
		    			inputStream.readFromEndInto(outputStream);
		    			//parser is not "ready for data" and requires export to be called
		    			//this export will populate the index positions for the JSON fields

		    			visitor.export(outputStream, extractor.getIndexPositions());
		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());
		    			
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
				    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    		} else {
		    			//send what data we have
		    			logger.debug("Unable to parse JSON");		    			
		    			
	    			    final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			
		    			Pipe.addLongValue(conId, localOutput);
		    			Pipe.addIntValue(userSessionId, localOutput);
		    			Pipe.addIntValue(contextFlags, localOutput);
		    			
		    			//moves the index data as is and must happen before JSON updates index
		    			inputStream.readFromEndInto(outputStream);   			
		    			
		    			///we could not parse the JSON so we have not written anything to the payload
		    			///the caller will see a zero length response for this call
		    			
		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());		    			
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
				    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    			
		    			visitor.clear();//reset for next JSON
		    		}
		        }	
		        break;
		        
		    	case NetResponseSchema.MSG_CONTINUATION_102:
		        {
		        	//hold to pass forward
		        	long conId = Pipe.takeLong(localInput);
		        	int userSessionId = Pipe.takeInt(localInput);
		        	int contextFlags = Pipe.takeInt(localInput);
		        	DataInputBlobReader<NetResponseSchema> inputStream = Pipe.openInputStream(localInput); //payload
	       			        	
		        	//copies params and headers.
		        	DataOutputBlobWriter<NetResponseSchema> outputStream = Pipe.openOutputStream(localOutput);
		        	inputStream.readInto(outputStream, inputStream.readFromEndLastInt(StructuredReader.PAYLOAD_INDEX_LOCATION));
		    
		        	//inputStream is now positioned to the JSON
		        	//outputStream is now positions as the target
		        	DataInputBlobReader.setupParser(inputStream, reader);
		    		parser.parse(reader, extractor.trieParser(), visitor);
		    		
		    		//if (TrieParserReader.parseHasContent(reader)) {
		    		//	logger.info("calls detected with {} bytes after JSON.",TrieParserReader.parseHasContentLength(reader));
		    		//}
		    		
		    		if (!visitor.isReady()  && visitor.isValid()) {
		    			
		    			final int size = Pipe.addMsgIdx(localOutput, msgIdx);
		    			
		    			Pipe.addLongValue(conId, localOutput);
		    			Pipe.addIntValue(userSessionId, localOutput);
		    			Pipe.addIntValue(contextFlags, localOutput);
		    			
		    			//moves the index data as is and must happen before JSON updates index
		    			inputStream.readFromEndInto(outputStream);
		    			//parser is not "ready for data" and requires export to be called
		    			//this export will populate the index positions for the JSON fields

		    			visitor.export(outputStream, extractor.getIndexPositions());
		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());
		    			
		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
				    			
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
		    		} else {
		    			//waiting for more in the next chunk.
		    			
		    			//TODO: how to detect  a failure vs needing more data...
		    			throw new UnsupportedOperationException("Not yet finished, needs implmentation for detection of corrupt data inside chunked data processing.");
		    			
//		    			//send what data we have
//		    			logger.debug("Unable to parse JSON");		    			
//		    			
//	    			    final int size = Pipe.addMsgIdx(localOutput, msgIdx);
//		    			
//		    			Pipe.addLongValue(conId, localOutput);
//		    			Pipe.addIntValue(userSessionId, localOutput);
//		    			Pipe.addIntValue(contextFlags, localOutput);
//		    			
//		    			//moves the index data as is and must happen before JSON updates index
//		    			inputStream.readFromEndInto(outputStream);   			
//		    			
//		    			///we could not parse the JSON so we have not written anything to the payload
//		    			///the caller will see a zero length response for this call
//		    			
//		    			DataOutputBlobWriter.commitBackData(outputStream, extractor.getStructId());		    			
//		    			DataOutputBlobWriter.closeLowLevelField(outputStream);
//				    			
//		    			Pipe.confirmLowLevelWrite(localOutput,size);
//		    			Pipe.publishWrites(localOutput);
//		    			
//		    			visitor.clear();//reset for next JSON
		    		}
		    		
		    		
		        }	
		        break;
		    	case NetResponseSchema.MSG_CLOSED_10:
			    	{
						long conId = Pipe.takeLong(input);
						int session = Pipe.takeInt(input);
			    		final int size = Pipe.addMsgIdx(localOutput, msgIdx);
			    		 
			    		ChannelReader hostReader = Pipe.openInputStream(localInput);		    		
			    		ChannelWriter hostWriter = Pipe.openOutputStream(localOutput);
			    		hostReader.readInto(hostWriter, hostReader.available());
			    		hostWriter.closeLowLevelField();
			    		
			    		Pipe.addIntValue(Pipe.takeInt(localInput), localOutput);
			    		
		    			Pipe.confirmLowLevelWrite(localOutput,size);
		    			Pipe.publishWrites(localOutput);
				    	break;
			    	}	
		        case -1:
		           requestShutdown();
		        break;
		        default:
		        	throw new UnsupportedOperationException("unknown message "+msgIdx);
		       
		    }
		    Pipe.confirmLowLevelRead(localInput, Pipe.sizeOf(localInput, msgIdx));
		    Pipe.releaseReadLock(localInput);
		}			
	}

}
