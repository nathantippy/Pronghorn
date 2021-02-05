package com.javanut.pronghorn.stage.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;


////if you want to mount a local drive to your ubuntu box these are the instructions	
//	sudo apt-get install cifs-utils
//	sudo mount -t cifs //moses.local/twelvedata /home/nate/moses
	

public class FileLoadStage extends PronghornStage {

	private final Pipe<RawDataSchema> input; 
    private final Pipe<RawDataSchema> output;
    private final File sourceFolder;
    
	public static FileLoadStage newInstance(GraphManager graphManager, String sourceFolder, 
								            Pipe<RawDataSchema> input, 
								            Pipe<RawDataSchema> output) {
		return new FileLoadStage(graphManager, sourceFolder, input, output);		
	}
	
	protected FileLoadStage(GraphManager graphManager, String sourceFolder, Pipe<RawDataSchema> input, Pipe<RawDataSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.sourceFolder = new File(sourceFolder);
		
		//not needed but we are trying some things 
	    //graphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
	}

	@Override
	public void run() {

		while (Pipe.hasContentToRead(input) &&  Pipe.hasRoomForWrite(output) ) {
			
			int idx = Pipe.takeMsgIdx(input);
			if (idx<0) {
				Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, idx));
				Pipe.releaseReadLock(input);
				Pipe.publishEOF(output);
				requestShutdown(); //TODO: this shutdown castcades to scripted run!!!! causing problem..
				return;
			} else {
				DataInputBlobReader<RawDataSchema> path = Pipe.openInputStream(input);		
				writeBody(path.readUTFFully());
				Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, idx));
				Pipe.releaseReadLock(input);
	    	}
		}
	}

	private void writeBody(String subPath) {
		Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		try {
			File target = new File(sourceFolder, subPath);
			InputStream inputStream = new FileInputStream(target);
			DataOutputBlobWriter<RawDataSchema> outputStream = Pipe.openOutputStream(output);
			int len = outputStream.writeStream(inputStream, (int)target.length());
			assert(len == target.length()) : "did not fully read file";
			DataOutputBlobWriter.closeLowLevelField(outputStream);
		} catch (FileNotFoundException e) {
			Pipe.addNullByteArray(output);				
		} catch (Exception e) {				
			throw new RuntimeException(e);
		}
		Pipe.confirmLowLevelWrite(output);
		Pipe.publishWrites(output);
	}
}
