package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Reads a Pronghorn "tape" from disk and writes it back onto a pipe.
 * Tape is a file format that mimics Pronghorn pipe data format, useful for
 * very structured formats.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class TapeReadStage extends PronghornStage {

    private final RandomAccessFile inputFile;
    private FileChannel fileChannel;

    //    private HeaderWritableByteChannel   HEADER_WRAPPER = new HeaderWritableByteChannel();    
    
    private IntBuferWritableByteChannel INT_BUFFER_WRAPPER;
    private final Pipe<RawDataSchema> target;    
    
    private int blobToRead=0;
    private int slabToRead=0;
    private long targetSlabPos;
    private int targetBlobPos;
    private ByteBuffer header;
    private  IntBuffer intHeader;
    private int slabInProgress = -1;
    private boolean shutdownInProgress;
    
    //TODO: add command pipe for reading from multiple channels
    //TODO: Unrelated: build stage with executor service as arg for map reduce using new random access to pipe

    /**
     *
     * @param graphManager
     * @param inputFile
     * @param output _out_ Writes the read tape directly back onto a RawDataSchema pipe.
     */
    public TapeReadStage(GraphManager graphManager, RandomAccessFile inputFile, Pipe<RawDataSchema> output) {
        super(graphManager, NONE, output);
        this.inputFile = inputFile;
        this.target = output;       

        this.supportsBatchedRelease=false;
        this.supportsBatchedPublish=false;
    }

    @Override
    public void startup() {
        fileChannel = inputFile.getChannel();
        header = ByteBuffer.allocate(8);
        ((Buffer)header).clear();
        intHeader = header.asIntBuffer();
        INT_BUFFER_WRAPPER = new IntBuferWritableByteChannel();
        
        targetBlobPos = Pipe.getWorkingBlobHeadPosition(target);
        targetSlabPos = Pipe.workingHeadPosition(target);  
    }
    
    @Override
    public void run() {
    	if (shutdownInProgress) {
    		if (!Pipe.hasRoomForWrite(target, Pipe.EOF_SIZE)) {
    			return;
    		}
    		requestShutdown();
    		return;    		
    	}
    	
        while (processAvailData(this)) {
            //keeps going while there is data to read and room to write it.
        }
    }

    @Override
    public void shutdown() {
        //if file contains eof it is never sent to pipe so we end with this one.
        Pipe.publishEOF(target);
    }
    private boolean processAvailData(TapeReadStage tapeReadStage) {

    	
    	
    	
        try {
        	
                        
            //read blob count int  (in bytes)
            //read slab count int  (in bytes)
            //read blob bytes
            //read slab ints
            
            if (0==slabToRead && 0==blobToRead) {

                int len = fileChannel.read(header);
                                
                if (len<0) {
                    fileChannel.close();
                    Pipe.publishAllBatchedWrites(target);
                    shutdownInProgress = true;
                    return false;
                }                
                if (header.hasRemaining()) {
                    //try again we did not get all 8 bytes.
                    return false;
                }
                
                ((Buffer)intHeader).clear();                
                blobToRead = intHeader.get();
                slabToRead = intHeader.get();                

                assert(slabToRead>0);

                ((Buffer)header).clear();                      
                
                if ((slabToRead>>2) >= target.sizeOfSlabRing) {
                    throw new UnsupportedOperationException("Unable to read file into short target pipe. The file chunks are larger than the pipe, please define a pipe to hold at least "+(slabToRead>>2)+" messages.");
                }      
                if (blobToRead >= target.sizeOfBlobRing) {
                    throw new UnsupportedOperationException("Unable to read file into short target pipe. The file chunks are larger than the pipe, please define a pipe to hold at least "+((int)Math.ceil(blobToRead /(float)target.sizeOfBlobRing)  )+"x longer varable data.");
                }      

                slabInProgress = slabToRead;//for confirmation of write
                
            }
            
            
            if (blobToRead>0) {
                //may take multiple passes until the blob is read into the ring buffer
                //  * may hit wrap boundary causing a second read
                //  * drive may not provide all the data at once causing more reads
                //  * may need to wait for free space on ring
                //
                ByteBuffer byteBuff = Pipe.wrappedBlobRingA(target);  //Get the blob array as a wrapped byte buffer     
                ((Buffer)byteBuff).clear();
                
                int blobMask = Pipe.blobMask(target);
                
                //NOTE: this is the published tail position and may be the most expensive call if we have contention, could be cached if this becomes a problem.
                int tail = Pipe.getBlobRingTailPosition(target) & blobMask;
                
                int writeToPos = targetBlobPos & blobMask; //Get the offset in the blob where we should write
                ((Buffer)byteBuff).position(writeToPos);   
                
                if (writeToPos < tail) {
                	((Buffer)byteBuff).limit(Math.min(tail, writeToPos + blobToRead ));
                } else {
                	((Buffer)byteBuff).limit(Math.min(byteBuff.capacity(), writeToPos +  blobToRead ));
                }                                
                
                int count = fileChannel.read(byteBuff);
                if (count<0) {
                    throw new UnsupportedOperationException("Unexpected end of file");
                }
                targetBlobPos += count;
                if ((blobToRead -= count)>0) {
                    return false; //try again later
                }
            }
            
            if (0==blobToRead && slabToRead>0) {
                          
                IntBuffer slabBuffer = Pipe.wrappedSlabRing(target);
                ((Buffer)slabBuffer).clear();
                                
                int slabMask = Pipe.slabMask(target);
                //NOTE: this is the published tail position and may be the most expensive call if we have contention, could be cached if this becomes a problem.
                int tail = (int)Pipe.tailPosition(target) & slabMask;
   
                int writeToPos = (int)targetSlabPos & slabMask;
                ((Buffer)slabBuffer).position( writeToPos );

                int slabToReadInts = slabToRead>>2;
                if (writeToPos < tail) {
                	((Buffer)slabBuffer).limit(Math.min(tail, writeToPos + slabToReadInts ));
                } else {
                	((Buffer)slabBuffer).limit(Math.min(slabBuffer.capacity(), writeToPos + slabToReadInts ));
                }
    
                long count = fileChannelRead(slabBuffer);
                if (count<0) {
                    throw new UnsupportedOperationException("Unexpected end of file");                    
                } 

                targetSlabPos += count;
                if ((slabToRead -= count)>0) {
                   return false;
                } 
            }
            
            if (0==slabToRead && 0==blobToRead && slabInProgress>=0) {
                
                Pipe.setBlobWorkingHead(target, targetBlobPos&Pipe.BYTES_WRAP_MASK);
                Pipe.setBlobHeadPosition(target, targetBlobPos&Pipe.BYTES_WRAP_MASK);
                
                Pipe.publishWorkingHeadPosition(target, targetSlabPos);
                               
                //only set this AFTER we have established the head positions.
                Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, RawDataSchema.MSG_CHUNKEDSTREAM_1));
                slabInProgress=-1;
            }
             
            
        } catch (IOException e) {
            e.printStackTrace();
            
            return false;
        }
        
        return true;
        
    }

    /**
     * Special method for reading data into an IntBuffer, this is done to minimize data copy
     */
    private long fileChannelRead(IntBuffer slabBuffer) throws IOException {

        long filePos = fileChannel.position();  
        long count = fileChannel.transferTo(filePos, slabBuffer.remaining()<<2 /*in bytes*/, INT_BUFFER_WRAPPER.init(slabBuffer));
        fileChannel.position(filePos+=count);

        return count;
    }

    private static class IntBuferWritableByteChannel implements WritableByteChannel {

        private IntBuffer buffer;
        
        public IntBuferWritableByteChannel init(IntBuffer intBuffer) {
            buffer = intBuffer;
            return this;
        }
        
        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() throws IOException {
        }
        
        @Override
        public int write(ByteBuffer src) throws IOException {  
            
            IntBuffer localBuffer = buffer;
            int count = Math.min( src.remaining()>>2, localBuffer.remaining()  );
            int i = count;
            while (--i>=0) {
                //old
                localBuffer.put(src.getInt());
                
                //new
                //localBuffer.put(readPackedInt(src));
            }
            return count<<2;
        }
        
        public static <S extends MessageSchema> int readPackedInt(ByteBuffer src) {
            byte v = src.get();
            int accumulator = (~((int)(((v>>6)&1)-1)))&0xFFFFFF80; 
            return (v >= 0) ? readPackedInt((accumulator | v) << 7,src) : accumulator |(v & 0x7F);
        }
        
        private static <S extends MessageSchema> int readPackedInt(int a, ByteBuffer src) {
            return readPackedIntB(a, src, src.get());
        }

        private static <S extends MessageSchema> int readPackedIntB(int a, ByteBuffer src, byte v) {
            assert(a!=0 || v!=0) : "malformed data";
            return (v >= 0) ? readPackedInt((a | v) << 7, src) : a | (v & 0x7F);
        }
        
        
        
    }

}
