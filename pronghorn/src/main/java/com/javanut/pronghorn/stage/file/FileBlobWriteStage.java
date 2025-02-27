package com.javanut.pronghorn.stage.file;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.pipe.util.ISOTimeFormatterLowGC;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

//TODO: should roll up writes when possible.
//TODO: update to use byteBuffer array...

/**
 * Writes data to file on disk (in blobs).
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class FileBlobWriteStage extends PronghornStage{

    private static final long FILE_ROTATE_SIZE = 1L<<27;
	private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
    private static final Logger logger = LoggerFactory.getLogger(FileBlobWriteStage.class);
    
    private final Pipe<RawDataSchema> input;
    private FileChannel fileChannel;
    
    private ByteBuffer buffA;
    private ByteBuffer buffB;
    private boolean releaseRead = false;
    
    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> writeOptions;
    
    ////////////////////////
    //file patterns  <BasePath>YYYYMMDDHHMMsssss.log
    //old files are deleted while running
    //upon restart the old files are NOT deleted.
    //keep ring of old files to be deleted.
    ////////////////////////
    
    private final int maxFileCount;
    private String[] absoluteFileNames;//so the old one can be deleted
    private final String basePath;

    private int selectedFile = 0;
    private final long fileRotateSize;
    private final String extension;
    
    private final boolean append;
    private StringBuilder pathBuilder;
    private ISOTimeFormatterLowGC formatter;

    public static FileBlobWriteStage newInstance(GraphManager graphManager,
							              Pipe<RawDataSchema> input,
							              String outputPathString) {
    	return new FileBlobWriteStage(graphManager, input, false, outputPathString);
    }
    
    /**
     *
     * @param graphManager
     * @param input _in_ RawDataSchema that will be written  to file.
     * @param append
     * @param outputPathString
     */
    public FileBlobWriteStage(GraphManager graphManager,
            Pipe<RawDataSchema> input,
            //add pipe to select file.
            boolean append, 
            String outputPathString) {
    	this(graphManager, input, FILE_ROTATE_SIZE, append, outputPathString, 1);
    }
    
    //fileRotateSize is ignored if there is only 1 file in the output path strings
    public FileBlobWriteStage(GraphManager graphManager,
    		                  Pipe<RawDataSchema> input,
    		                  long fileRotateSize, //ignored if file count is 1
    		                  boolean append, 
    		                  String pathBase,
    		                  int maxFileCount) {
    	
    	
    	//TODO: add second constructor to add control pipe.
    	
        super(graphManager, input, NONE);
        assert(pathBase!=null);
        this.append = append;
        this.fileRotateSize = fileRotateSize;
        this.input = input;
        this.maxFileCount = maxFileCount;
        this.basePath = pathBase;
        this.extension = ".log";
        
        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "fileio", stageId);
  
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        long LARGE_SLA_FOR_FILE_WRITE = 10_000_000_000L;
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, LARGE_SLA_FOR_FILE_WRITE, this);
        
        
    }

    @Override
    public void startup() {

    	this.formatter = new ISOTimeFormatterLowGC(true);
    			
        this.fileSystem = FileSystems.getDefault();
        this.provider = fileSystem.provider();
        this.writeOptions = new HashSet<OpenOption>();

        this.writeOptions.add(StandardOpenOption.SYNC);
        this.writeOptions.add(StandardOpenOption.CREATE);

        if (append) {
        	this.writeOptions.add(StandardOpenOption.APPEND);        	
        } else {
        	this.writeOptions.add(StandardOpenOption.TRUNCATE_EXISTING);
        }
        
        this.writeOptions.add(StandardOpenOption.WRITE);
        
        this.absoluteFileNames = new String[maxFileCount];
        this.pathBuilder = new StringBuilder();
        
        try {
        	assert(0==selectedFile);
        	this.absoluteFileNames[selectedFile] =  generateFileName(extension);
        	        	
        	fileChannel = provider.newFileChannel(
        			fileSystem.getPath(this.absoluteFileNames[selectedFile]), writeOptions);
        
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        
    }    
    
    
    private String generateFileName(String ext) {
    	
    	if (maxFileCount==1) {
    		return basePath;//no rotation or timestamps
    	} else {
    		pathBuilder.setLength(0);
    		pathBuilder.append(basePath);
    		formatter.write(System.currentTimeMillis(), pathBuilder);
    		pathBuilder.append(ext);
    		return pathBuilder.toString();
    	}
    	
	}

	@Override
    public void run() {
             
        writeProcessing();
        
    }

	private void writeProcessing() {
		do {
        
        if (null==buffA && 
            null==buffB) {
            //read the next block
            
            if (releaseRead) {
                //only done after we have consumed the bytes
                Pipe.confirmLowLevelRead(input, SIZE);
                Pipe.releaseReadLock(input);
                releaseRead = false;
            }

            //only rotate when we have more than 1 file 
            if (maxFileCount>1) {
	            try {
					long fileSize = fileChannel.size();
					if (fileSize>fileRotateSize) {
						//close file
						fileChannel.close();
						
						//rotate to next file.
						if (++selectedFile == maxFileCount) {
							selectedFile = 0;
						}
						
						String oldName = "";
						if (null!=absoluteFileNames[selectedFile]) {
							//before we replace this file we must delete if it is found							
							provider.delete(fileSystem.getPath(oldName = absoluteFileNames[selectedFile]));							
						}
						
						absoluteFileNames[selectedFile] = generateFileName(extension);
						if (absoluteFileNames[selectedFile].equals(oldName)) {
							logger.warn("log file names are not unique because the file sizes are too small, increase max size.");
							//to allow for continued use modify the name to avoid collision.
							absoluteFileNames[selectedFile] += (""+Math.random());
						}
						
						fileChannel = provider.newFileChannel(
			        			 fileSystem.getPath(absoluteFileNames[selectedFile]), writeOptions);
					}
					
					
				} catch (IOException e) {
					
					//do not report closed when we are shutting down
					if (!(e instanceof ClosedChannelException)) {
						throw new RuntimeException(e);
					}
					
				}
            }
            
            
            if (Pipe.hasContentToRead(input)) {
                int msgId      = Pipe.takeMsgIdx(input);   
                if (msgId < 0) {
                    Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
                    Pipe.releaseReadLock(input);
                    requestShutdown();
                    return;
                }
                assert(0==msgId);
                int meta = Pipe.takeByteArrayMetaData(input); //for string and byte array
                int len = Pipe.takeByteArrayLength(input);
                                
                if (len < 0) {
                    Pipe.confirmLowLevelRead(input, SIZE);
                    Pipe.releaseReadLock(input);
                    requestShutdown();
                    return;
                }
                
                                                
                releaseRead = true;
                buffA = Pipe.wrappedBlobReadingRingA(input, meta, len);
                buffB = Pipe.wrappedBlobReadingRingB(input, meta, len);
                if (!((Buffer)buffB).hasRemaining()) {
                    buffB = null;
                }
                
                
            } else {
                //there is nothing to read
                return;
            }
            
        }
        
        //we have existing data to be written
        if (null!=buffA) {
            try {
                
                 fileChannel.write(buffA);
                if (0==buffA.remaining()) {
                    buffA = null;
                } else {
                    return;//do not write B because we did not finish A
                }
                
                if (this.didWorkMonitor != null) {
        			didWorkMonitor.published(); //did work but we are batching.
        		}
                
            } catch (IOException e) {
            	
            	//TODO: must revisit on shutdown so we dont write when file is closed...
            	
                throw new RuntimeException(e);
            }
        }
        
        if (null!=buffB) {
            try {                
                fileChannel.write(buffB);
                if (0==buffB.remaining()) {
                    buffB = null;
                }
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        } while (null == buffA && null == buffB);
	}

    @Override
    public void shutdown() {
        if (releaseRead) {
            //only done after we have consumed the bytes
            Pipe.releaseReadLock(input);
        }
        if (fileChannel.isOpen()) {
        	try {
				fileChannel.close();
			} catch (IOException e) {
				logger.info("unable to close ",e);
			}
        }
                
    }
    
}
