package com.javanut.pronghorn.stage.lzma;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class LZMADecompressStage extends PronghornStage{

	private final Pipe<RawDataSchema> input; 
	private final Pipe<RawDataSchema> output;
       
	
	public static LZMADecompressStage newInstance(GraphManager graphManager, 
            Pipe<RawDataSchema> input, 
            Pipe<RawDataSchema> output) {
		
		return new LZMADecompressStage(graphManager, input, output);
		
	}
	
	protected LZMADecompressStage(GraphManager graphManager, Pipe<RawDataSchema> input, Pipe<RawDataSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		
	}

		
	@Override
	public void run() {
				
	
		while (Pipe.hasRoomForWrite(output) && Pipe.hasContentToRead(input)) {
			int idx = Pipe.takeMsgIdx(input);
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
            	
            	ChannelWriter outputStream = Pipe.openOutputStream(output);
            	decodeSingleLZMABlock(inputStream, outputStream);
            	outputStream.closeLowLevelField();
            	
            }
			Pipe.confirmLowLevelWrite(output);
			Pipe.publishWrites(output);
            
            Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, idx));
            Pipe.releaseReadLock(input);
			
			
		}
		
		
		
	}
	
	@Override
	public void startup() {
		properties = new byte[propertiesSize];
		decoder = new SevenZip.Compression.LZMA.Decoder();
	}
	
	static final int propertiesSize = 5;
	private byte[] properties;
	private SevenZip.Compression.LZMA.Decoder decoder;
	
	private final void decodeSingleLZMABlock(InputStream inStream, OutputStream outStream) {
		try {
			if (inStream.read(properties, 0, propertiesSize) != propertiesSize) {
				throw new RuntimeException("input .lzma file is too short");
			}
			if (!decoder.SetDecoderProperties(properties)) {
				throw new RuntimeException("Incorrect stream properties");
			}
			
			long outSize = 0;
			for (int i = 0; i < 8; i++)
			{
				int v = inStream.read();
				if (v < 0) {
					throw new RuntimeException("Can't read stream size");
				}
				outSize |= ((long)v) << (8 * i);
			}
			if (!decoder.Code(inStream, outStream, outSize)) {
				throw new RuntimeException("Error in data stream");
			}
		} catch (IOException ioex) {
			throw new RuntimeException(ioex);
		}
	}

}
