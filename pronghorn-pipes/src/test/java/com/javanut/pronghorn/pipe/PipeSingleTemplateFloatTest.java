package com.javanut.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchemaDynamic;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeConfig;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.pipe.PipeWriter;
import com.javanut.pronghorn.pipe.token.OperatorMask;
import com.javanut.pronghorn.pipe.token.TokenBuilder;
import com.javanut.pronghorn.pipe.token.TypeMask;

public class PipeSingleTemplateFloatTest {

    private static int[] SINGLE_MESSAGE_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.IntegerUnsigned, 
																			       OperatorMask.Field_None, 
																			       0)};
	private static String[] SINGLE_MESSAGE_NAMES = new String[]{"Float"};
	private static long[] SINGLE_MESSAGE_IDS = new long[]{0};
	private static final short ZERO_PREMABLE = 0;
	public static final FieldReferenceOffsetManager FLOAT_SCRIPT = new FieldReferenceOffsetManager(SINGLE_MESSAGE_TOKENS, 
																					              ZERO_PREMABLE, 
																					              SINGLE_MESSAGE_NAMES, 
																					              SINGLE_MESSAGE_IDS);
	
	
	final FieldReferenceOffsetManager FROM = FLOAT_SCRIPT;
	final int FRAG_LOC = 0;
	
    @Test
    public void simpleWriteRead() {
    
    	byte primaryRingSizeInBits = 9; 
    	byte byteRingSizeInBits = 18;
    	
		Pipe ring = new Pipe(new PipeConfig(new MessageSchemaDynamic(FROM), 1<<primaryRingSizeInBits, 1<<byteRingSizeInBits));
    	ring.initBuffers();
    	
        int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        int varDataMax = (ring.blobMask/(ring.slabMask>>1))/messageSize;        
        int testSize = ((1<<primaryRingSizeInBits)/messageSize)-1; //room for EOF

        writeTestValue(ring, varDataMax, testSize);
        
        //now read the data back        
        int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);
        
        
        int k = testSize;
        while (PipeReader.tryReadFragment(ring)) {
        	
        	--k;
        	assertTrue(PipeReader.isNewMessage(ring));
			int messageIdx = PipeReader.getMsgIdx(ring);
			if (messageIdx<0) {
				return;
			}
			testReadValue(ring, varDataMax, testSize, FIELD_LOC, k, messageIdx);
 
        }    
    }

	private void testReadValue(Pipe pipe, int varDataMax, int testSize,
			int FIELD_LOC, int k, int messageIdx) {
		assertEquals(0, messageIdx);
		
		
		float expectedValue = 1f/(float)((varDataMax*(k))/testSize);		        	
		float value = PipeReader.readIntBitsToFloat(pipe, FIELD_LOC);	
		assertEquals(expectedValue, value, .00001);
	}

	private void writeTestValue(Pipe pipe, int blockSize, int testSize) {
		
		int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);
		assertTrue(0==Pipe.contentRemaining(pipe));
		int j = testSize;
        while (true) {
        	        	
        	if (j == 0) {
        		PipeWriter.publishEOF(pipe);
        		return;//done
        	}
               	        	
        	if (PipeWriter.tryWriteFragment(pipe, FRAG_LOC)) { //returns true if there is room to write this fragment
        		
        		int value = (--j*blockSize)/testSize;        		        		
        		PipeWriter.writeFloatAsIntBits(pipe, FIELD_LOC, 1f/(float)value);        		
        		PipeWriter.publishWrites(pipe); //must always publish the writes if message or fragment
        		        		
        	} else {
        		//Unable to write because there is no room so do something else while we are waiting.
        		Thread.yield();
        	}        	
        	
        }
        
        
        
	}
    
    @Test
    public void simpleWriteReadThreaded() {
    
    	final byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	final byte byteRingSizeInBits = 16;
    	final Pipe ring = new Pipe(new PipeConfig(new MessageSchemaDynamic(FROM), 1<<primaryRingSizeInBits, 1<<byteRingSizeInBits));
    	ring.initBuffers();
    	
        final int messageSize = FROM.fragDataSize[FRAG_LOC];
        
        final int varDataMax = (ring.blobMask/(ring.slabMask>>1))/messageSize;        
        final int testSize = (1<<primaryRingSizeInBits)/messageSize;
                
    	Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				writeTestValue(ring, varDataMax, testSize);
			}}
			);
    	t.start();
        
        //now read the data back
         
        
        int FIELD_LOC = FieldReferenceOffsetManager.lookupFieldLocator(SINGLE_MESSAGE_NAMES[0], FRAG_LOC, FROM);
        
        int k = testSize;
        while (k>0) {
        	
        	//This is the example code that one would normally use.
        	
        	//System.err.println("content "+ring.contentRemaining(ring));
	        if (PipeReader.tryReadFragment(ring)) { //this method releases old messages as needed and moves pointer up to the next fragment
	        	k--;//count down all the expected messages so we stop this test at the right time

	        	assertTrue(PipeReader.isNewMessage(ring));
				int messageIdx = PipeReader.getMsgIdx(ring);
				if (messageIdx<0) {
					return;
				}
				testReadValue(ring, varDataMax, testSize, FIELD_LOC, k, messageIdx);
	        	
	        } else {
	        	//unable to read so at this point
	        	//we can do other work and try again soon
	        	Thread.yield();
	        	
	        }
        }
                
        }    
}