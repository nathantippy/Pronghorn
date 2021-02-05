package com.javanut.pronghorn.stage.lzma;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

import SevenZip.LzmaAlone.CommandLine;
import SevenZip.Compression.LZMA.Base;

public class LZMACompressStage extends PronghornStage{

	
	private final Pipe<RawDataSchema> input; 
	private final Pipe<RawDataSchema> output;
    
	public static LZMACompressStage newInstance(GraphManager graphManager, 
            Pipe<RawDataSchema> input, 
            Pipe<RawDataSchema> output) {
		
		return new LZMACompressStage(graphManager, input, output);
		
	}
	
	protected LZMACompressStage(GraphManager graphManager, Pipe<RawDataSchema> input, Pipe<RawDataSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
	}

	@Override
	public void run() {

		while (Pipe.hasRoomForWrite(output) && Pipe.hasContentToRead(input)) {
			
			int idx = Pipe.takeMsgIdx(input);
			
			//System.out.println("idx: "+idx);
			
			if (idx<0) {
	
				Pipe.confirmLowLevelRead(input,  Pipe.sizeOf(input, idx));
				Pipe.releaseReadLock(input);

				Pipe.publishEOF(output);
								
				requestShutdown();
				return;
			}
			ChannelReader inputStream = Pipe.openInputStream(input);
            
            Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
            if (inputStream.available()<=0) {
            	//write nothing 
            	Pipe.addNullByteArray(output);       	
            	
            } else {
            	
            	
            	DataOutputBlobWriter<RawDataSchema> outputStream = Pipe.openOutputStream(output);
            	
            	encodeSingleLZMABlock(inputStream.available(), inputStream, outputStream);
            	int size = outputStream.closeLowLevelField();
                 	
            	//System.out.println("                comp size: "+size);
            }
			Pipe.confirmLowLevelWrite(output);
			Pipe.publishWrites(output);
            
            Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, idx));
            Pipe.releaseReadLock(input);
			
			
		}
		
	}

	
	private static final void encodeSingleLZMABlock(long inputSize, InputStream inStream, OutputStream outStream) {
		try {
			CommandLine params = new CommandLine();
			SevenZip.Compression.LZMA.Encoder encoder = new SevenZip.Compression.LZMA.Encoder();
			encoder.SetAlgorithm(2); // >=2 is max mode
			encoder.SetDictionarySize(4096);
			encoder.SetNumFastBytes(Base.kMatchMaxLen);

			encoder.SetMatchFinder(0);  //  0 168.7  10:59.410
			                            //  1 168.7  12.03.217
			                            //  2 168.7  11:55.552


			encoder.SetLcLpPb(Base.kNumLitPosStatesBitsEncodingMax, Base.kNumLitContextBitsMax, Base.kNumPosStatesBitsEncodingMax);
			encoder.SetEndMarkerMode(params.Eos);
			encoder.WriteCoderProperties(outStream);
			long fileSize;
			if (params.Eos) {
				fileSize = -1;
			} else {
				fileSize = inputSize;
			}
			for (int i = 0; i < 8; i++) {
				outStream.write((int)(fileSize >>> (8 * i)) & 0xFF);
			}
			encoder.Code(inStream, outStream, -1, -1, null);
		} catch (IOException ioex) {
			throw new RuntimeException(ioex);
		}
	}
	
}
