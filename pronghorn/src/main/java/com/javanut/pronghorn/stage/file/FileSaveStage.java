package com.javanut.pronghorn.stage.file;

import java.io.File;
import java.io.FileOutputStream;

import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class FileSaveStage extends PronghornStage {

	private final Pipe<RawDataSchema> wherePath; 
	private final Pipe<RawDataSchema> body;
	private final File targetFolder;
		
	public static FileSaveStage newInstance(GraphManager graphManager, File targetFolder,
            Pipe<RawDataSchema> wherePath, Pipe<RawDataSchema> body) {
		return new FileSaveStage(graphManager, targetFolder, wherePath, body);
	}
	
	protected FileSaveStage(GraphManager graphManager, File targetFolder, Pipe<RawDataSchema> wherePath, Pipe<RawDataSchema> body) {
		super(graphManager, join(wherePath,body), NONE);
		this.wherePath = wherePath;
		this.body = body;
		this.targetFolder = targetFolder;
		assert(this.targetFolder.isDirectory());
		
		//graphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
	}

	@Override
	public void run() {
		
		while (Pipe.hasContentToRead(wherePath) && Pipe.hasContentToRead(body) ) {
			
			String desiredPath = "";
			int a = Pipe.takeMsgIdx(wherePath);
			if (a<0) {				
				//ignore
				Pipe.confirmLowLevelRead(wherePath, Pipe.sizeOf(wherePath,  a));
				Pipe.releaseReadLock(wherePath);
				
			} else {
				DataInputBlobReader<RawDataSchema> inputStream = Pipe.openInputStream(wherePath);	
				desiredPath = inputStream.readUTFFully();	
				//System.out.println("desired path: "+desiredPath);
				Pipe.confirmLowLevelRead(wherePath, Pipe.sizeOf(wherePath,  a));
				Pipe.releaseReadLock(wherePath);
			}
			
			
			int b = Pipe.takeMsgIdx(body);
            if (b<0) {
            	Pipe.confirmLowLevelRead(body, Pipe.sizeOf(body,  b));
    			Pipe.releaseReadLock(body);
    			
    			//System.out.println("shutdown file write!!!!!!");
				requestShutdown();
				return;
			} else {
				File target = new File(targetFolder, desiredPath);
				if (!target.exists()) {
					int idx = desiredPath.indexOf(File.separatorChar);
					if (idx>0) {
						new File(targetFolder, desiredPath.substring(0,idx)).mkdirs();						
					}					
				}
								
				FileOutputStream fost;
				try {
					fost = new FileOutputStream(target);
					Pipe.writeFieldToOutputStream(body, fost);				
								
					fost.close();				
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			Pipe.confirmLowLevelRead(body, Pipe.sizeOf(body,  b));
			Pipe.releaseReadLock(body);
		}
	}
}
