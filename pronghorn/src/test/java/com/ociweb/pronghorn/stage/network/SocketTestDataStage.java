package com.ociweb.pronghorn.stage.network;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * _no-docs_
 */
public class SocketTestDataStage extends PronghornStage {

	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<ReleaseSchema> releasePipe;
	private final boolean encrytpedContent;
	private final int testUsers;
	private final int[] testSeeds;
	private final int[] testSizes;
	private int expectedCountRemaining;
	private int[] bytesPerUser;
	private int expectedBytesPerUser;
		
	private final static Logger logger = LoggerFactory.getLogger(SocketTestDataStage.class);
	
	public SocketTestDataStage(GraphManager gm, Pipe<NetPayloadSchema>[] input, Pipe<ReleaseSchema> releasePipe, 
			                    boolean encryptedContent, int testUsers, int[] testSeeds, int[] testSizes) {
		super(gm,input,releasePipe);
		this.input = input;
		this.releasePipe = releasePipe;
		this.encrytpedContent = encryptedContent;
		this.testUsers=testUsers;
		this.testSeeds=testSeeds;
		this.testSizes=testSizes;
	}

	@Override
	public void startup() {
		
		this.bytesPerUser = new int[testUsers+1]; //tge id values are 1 base offset
				
		int sum = 0;
		//count all the bytes..
		int i = testSizes.length;
		while (--i>=0) {
			sum += testSizes[i];		
		}
		expectedBytesPerUser = sum;
		this.expectedCountRemaining = testUsers*expectedBytesPerUser;
				
	}
	
	@Override
	public void shutdown() {
		
		Assert.assertEquals(0, bytesPerUser[0]);
		for(int x=1;x<bytesPerUser.length;x++) { 
			Assert.assertEquals(expectedBytesPerUser, bytesPerUser[x]);
		}
		
	}
	
	@Override
	public void run() {
		
		int i = input.length;
		while (--i>=00) {
			int count = testNewData(input[i],releasePipe, encrytpedContent,bytesPerUser);
			expectedCountRemaining-=count;
			if (expectedCountRemaining==0 || count<0) {
				requestShutdown();
				return;
			}
		}
	}

	private static int testNewData(Pipe<NetPayloadSchema> pipe, Pipe<ReleaseSchema> release, boolean encrytpedContent, int[] bytesPerUser) {
		
		int count = 0;
		while (Pipe.hasContentToRead(pipe) && Pipe.hasRoomForWrite(release)) {
			
			int msgIdx = Pipe.takeMsgIdx(pipe);
			//logger.info("------- consume msg Idx: {}",msgIdx);
			
			switch (msgIdx) {
			
				case NetPayloadSchema.MSG_DISCONNECT_203:
					throw new UnsupportedOperationException("Not expected in test");
					//break;
				case NetPayloadSchema.MSG_ENCRYPTED_200:
					{
					//	logger.info("----- reading encrypted");
					Assert.assertTrue(encrytpedContent);
					long conId = Pipe.takeLong(pipe);
					long arrivalTime = Pipe.takeLong(pipe);
										
					int meta = Pipe.takeByteArrayMetaData(pipe);
					int len = Pipe.takeByteArrayLength(pipe);		
					int pos = Pipe.bytePosition(meta, pipe, len);
					
					//TODO: add test here for payload
					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_ENCRYPTED_200));
					Pipe.releaseReadLock(pipe);
					
					publishRelease(release, conId, Pipe.tailPosition(pipe));
					
					bytesPerUser[(int)conId] += len;
					
					count += len;
					}
					break;
				case NetPayloadSchema.MSG_PLAIN_210:
					{
				//	logger.info("----- reading plain");
						
					Assert.assertFalse(encrytpedContent);
					
					long conId = Pipe.takeLong(pipe);
					long arrivalTime = Pipe.takeLong(pipe);
										
					long position = Pipe.takeLong(pipe);
					

					int meta = Pipe.takeByteArrayMetaData(pipe);
					int len = Pipe.takeByteArrayLength(pipe);					
					int pos = Pipe.bytePosition(meta, pipe, len);
					

					//TODO: add test here for payload
					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
					Pipe.releaseReadLock(pipe);
					
					publishRelease(release, conId, position>=0?position:Pipe.tailPosition(pipe));
					
					bytesPerUser[(int)conId] += len;
					
					count += len;
					}	
					break;
				case NetPayloadSchema.MSG_UPGRADE_307:
					throw new UnsupportedOperationException("Not expected in test");
				case NetPayloadSchema.MSG_BEGIN_208:
					
					int seq = Pipe.takeInt(pipe);					
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance,NetPayloadSchema.MSG_BEGIN_208));
					Pipe.releaseReadLock(pipe);
										
					break;
				case -1:
					Pipe.confirmLowLevelRead(pipe, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(pipe);
					return -1;
			}	
		}
		return count;
		
	}

	private static void publishRelease(Pipe pipe, long conId, long position) {
		assert(position!=-1);
		int size = Pipe.addMsgIdx(pipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
		Pipe.addLongValue(conId, pipe);
		Pipe.addLongValue(position, pipe);
		Pipe.addIntValue(0, pipe);
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
	}
	
	

}
