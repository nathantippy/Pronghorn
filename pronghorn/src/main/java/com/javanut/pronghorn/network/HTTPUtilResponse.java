package com.javanut.pronghorn.network;

import com.javanut.pronghorn.network.config.HTTPContentType;
import com.javanut.pronghorn.network.http.HTTPUtil;
import com.javanut.pronghorn.network.http.HeaderWritable;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.Pipe.PaddedLong;

public class HTTPUtilResponse {
	
	public long block1PositionOfLen;
	public int block1HeaderBlobPosition;
	private long block1ConSeqSlabPos;

	public HTTPUtilResponse() {
	}


	public static void holdEmptyBlock( HTTPUtilResponse that,
					            long connectionId, 
					            final int sequenceNo,
					            Pipe<ServerResponseSchema> pipe) {
		
		holdEmptyBlock(that, connectionId, sequenceNo, pipe, pipe.maxVarLen);
	}


	public static void holdEmptyBlock(HTTPUtilResponse that, long connectionId, final int sequenceNo,
			Pipe<ServerResponseSchema> pipe, int emptyBlockSize) {
		Pipe.addMsgIdx(pipe, ServerResponseSchema.MSG_TOCHANNEL_100);			
		that.block1ConSeqSlabPos = Pipe.workingHeadPosition(pipe);

		//this is used by downstream tasks which may be peeking...
		Pipe.addLongValue(connectionId, pipe);
		Pipe.addIntValue(sequenceNo, pipe);	
		
		DataOutputBlobWriter<?> outputStream = Pipe.outputStream(pipe);	
		that.block1HeaderBlobPosition = Pipe.getWorkingBlobHeadPosition(pipe);

		DataOutputBlobWriter.openFieldAtPosition(outputStream, that.block1HeaderBlobPosition); 	//no context, that will come in the second message 
		
		//for the var field we store this as meta then length
		that.block1PositionOfLen = (1+Pipe.workingHeadPosition(pipe));
		
		//by making this smaller the pre-fetch will then work much better.
		DataOutputBlobWriter.closeLowLeveLField(outputStream, emptyBlockSize);
		
		
		assert(pipe.maxVarLen == Pipe.slab(pipe)[((int)that.block1PositionOfLen) & Pipe.slabMask(pipe)]) : "expected max var field length";
		
		Pipe.addIntValue(0, pipe); //not needed, this is set later
		//the full blob size of this message is very large to ensure we have room later...
		//this call allows for the following message to be written after this messages blob data
		int consumed = Pipe.writeTrailingCountOfBytesConsumed(outputStream.getPipe()); 
		assert(pipe.maxVarLen == consumed);
		Pipe.confirmLowLevelWrite(pipe); 
		//Stores this publish until the next message is complete and published
		Pipe.storeUnpublishedWrites(outputStream.getPipe());

		
		//logger.info("new empty block at {} {} ",block1HeaderBlobPosition, block1PositionOfLen);
	}

	public static void openToEmptyBlock(HTTPUtilResponse that, DataOutputBlobWriter<?> outputStream) {
		DataOutputBlobWriter.openFieldAtPosition(outputStream, that.block1HeaderBlobPosition);
	}

	public static void finalizeLengthOfFirstBlock(HTTPUtilResponse that, DataOutputBlobWriter<?> outputStream) {
		int propperLength = DataOutputBlobWriter.length(outputStream);
		Pipe.validateVarLength(outputStream.getPipe(), propperLength);
		Pipe.setIntValue(propperLength, outputStream.getPipe(), that.block1PositionOfLen); //go back and set the right length.
		outputStream.getPipe().closeBlobFieldWrite();
	}
	
	public static ChannelWriter openHTTPPayload(
			HTTPUtilResponse that, 
			Pipe<ServerResponseSchema> output, long activeChannelId, int activeSequenceNo) {
		HTTPUtilResponse.holdEmptyBlock(that, activeChannelId, activeSequenceNo, output);
		
		return Pipe.openOutputStream(output);
	}

	public static void closePayloadAndPublish(
			HTTPUtilResponse that, 
			byte[] eTag, HTTPContentType contentTypeEnum,
			Pipe<ServerResponseSchema> output, 
			long activeChannelId, int activeSequenceNo, int activeFieldRequestContext, 
			ChannelWriter outputStream, 
			HeaderWritable additionalHeaderWriter, int status) {
							
		storeChnlAndSeqInHeaderFragment(that, output, activeChannelId, activeSequenceNo);
		
		byte[] contentType = null!=contentTypeEnum ? contentTypeEnum.getBytes() : null;
		
		int totalLengthWritten = outputStream.length();
		//this is a key pronghorn pattern in use here
		output.closeBlobFieldWrite(); //closed because we will add each part below...
		HTTPUtil.prependBodyWithHeader(output, 
				              eTag, totalLengthWritten, that, activeFieldRequestContext,
				              activeChannelId,  activeSequenceNo, contentType, 
				              additionalHeaderWriter, status);//context
	}

	private static void storeChnlAndSeqInHeaderFragment(HTTPUtilResponse that,
			Pipe<ServerResponseSchema> output,
			long activeChannelId, int activeSequenceNo) {
		PaddedLong head = Pipe.getWorkingHeadPositionObject(output);
		long temp = head.value;		
		head.value = that.block1ConSeqSlabPos;		
		Pipe.addLongValue(activeChannelId, output);
		Pipe.addIntValue(activeSequenceNo, output);	
		head.value = temp;
	}
	
	public static boolean isBeginningOfResponse(int flags) {
		return 0 != (flags & ServerCoordinator.BEGIN_RESPONSE_MASK);
	}
	
	public static boolean isEndOfResponse(int flags) {
		return 0 != (flags & ServerCoordinator.END_RESPONSE_MASK);
	}
	
	public static boolean isConnectionClosed(int flags) {
		return 0 != (flags & ServerCoordinator.CLOSE_CONNECTION_MASK);
	}
}