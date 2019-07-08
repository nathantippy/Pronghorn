package com.javanut.pronghorn.util;

import com.javanut.json.decode.JSONExtractor;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.pipe.StructuredReader;
import com.javanut.pronghorn.struct.StructRegistry;
import com.javanut.pronghorn.util.CharSequenceToUTF8Local;
import com.javanut.pronghorn.util.TrieParser;
import com.javanut.pronghorn.util.TrieParserReader;
import com.javanut.pronghorn.util.TrieParserReaderLocal;
import com.javanut.pronghorn.util.parse.JSONStreamParser;
import com.javanut.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSON2Struct {

	private final Pipe<RawDataSchema> p;
	private final JSONStreamParser parser;
	private final TrieParser trieParser;
	private final JSONStreamVisitorToChannel streamToChannel;
	private final int[] idxPos;
	private final int structId;
	
	public JSON2Struct( int maximumLenghOfVariableLengthFields, JSONExtractor ex) {
		    StructRegistry recordTypeData = new StructRegistry();
	        streamToChannel = ex.newJSONVisitor();		        
	    	parser = new JSONStreamParser();
			p = RawDataSchema.instance.newPipe(2, maximumLenghOfVariableLengthFields);
	    	p.initBuffers();
	    	Pipe.structRegistry(p, recordTypeData);
	    	structId = recordTypeData.addStruct();					
	    	ex.addToStruct(Pipe.structRegistry(p), structId);
	    	trieParser = ex.trieParser();
	    	idxPos = ex.getIndexPositions();	    	
	}
	
	public boolean visit(String json, JSON2StructVisitor visitor) {
    	final TrieParserReader reader = TrieParserReaderLocal.get();
	    CharSequenceToUTF8Local.get().convert(json).parseSetup(reader);//TODO: find an easier way to say this..  
		parser.parse( reader, trieParser, streamToChannel);
		
		if (!streamToChannel.isReady() && streamToChannel.isValid()) {
								
			Pipe.addMsgIdx(p, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			ChannelWriter writer = Pipe.openOutputStream(p);					
			streamToChannel.export(writer, idxPos);					
			DataOutputBlobWriter.commitBackData((DataOutputBlobWriter<RawDataSchema>) writer, structId);
			writer.closeLowLevelField();
			Pipe.confirmLowLevelWrite(p);
			Pipe.publishWrites(p);
			
			Pipe.takeMsgIdx(p);
			DataInputBlobReader<RawDataSchema> in = Pipe.openInputStream(p);
		
			StructuredReader structured = in.structured();			
		    visitor.visit(structured);
			
			Pipe.confirmLowLevelRead(p, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
			Pipe.releaseReadLock(p);
			
			return true;
		} else {
			return false;	//unable to parse...		
		}
	}
}
