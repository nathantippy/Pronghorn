package com.ociweb.pronghorn.network.http;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xResponseParserStage extends PronghornStage {
	private static final int SIZE_OF_MSG_RESPONSE = Pipe.sizeOf(NetResponseSchema.instance, NetResponseSchema.MSG_RESPONSE_101);
	private final Pipe<NetPayloadSchema>[] input; 
	private final Pipe<NetResponseSchema>[] output;
	private int[] outputOwner; //tracking active use of the output
	
	private long[] inputPosition;
	private long[] arrivalTimeAtPosition;
	private long[] blockedPosition;
	private int[]  blockedOpenCount;
	private int[]  blockedLen;
	private int[]  blockedState;
	
	private final Pipe<ReleaseSchema> releasePipe;
	private final HTTPSpecification<?,?,?,?> httpSpec;
	
	private static final boolean showTossedData = false;
	
	private ClientCoordinator ccm;
	
		
	
	private static final Logger logger = LoggerFactory.getLogger(HTTP1xResponseParserStage.class);
	
	private static final int MAX_VALID_STATUS = 2048;
	private static final int MAX_VALID_HEADER = 32768; //much larger than most servers using 4-8K (pipe blob must be larger than this)

   	
	private TrieParser revisionMap;
	
	private TrieParserReader trieReader;
	private int[] positionMemoData;
	private long[] payloadLengthData;
	private long[] ccIdData;
	
	private int[] runningHeaderBytes;
			
	private int lastMessageType=-1;   //does not change
	private long lastPayloadSize=-1;  //does not change
	
	public static boolean showData = false;
	
	public HTTP1xResponseParserStage(GraphManager graphManager, 
			                       Pipe<NetPayloadSchema>[] input, 
			                       Pipe<NetResponseSchema>[] output, 
			                       Pipe<ReleaseSchema> ackStop,
			                       ClientCoordinator ccm,
			                       HTTPSpecification<?,?,?,?> httpSpec) {
		
		super(graphManager, input, join(output,ackStop));
		this.input = input;
		this.output = output;//must be 1 for each listener
		this.ccm = ccm;
		this.releasePipe = ackStop;
		this.httpSpec = httpSpec;		

		
		int i = input.length;
		while (--i>=0) {
			assert(	input[i].sizeOfBlobRing >=  MAX_VALID_HEADER*2 ); //size of blob ring is the largest a header can ever be.			
		}

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
		GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		GraphManager.addNota(graphManager, GraphManager.LOAD_BALANCER, GraphManager.LOAD_BALANCER, this);
		
	}

	  @Override
	    public void startup() {
		  
		  outputOwner = new int[output.length];
		  Arrays.fill(outputOwner, -1);
		  
	        		  
		  positionMemoData = new int[input.length<<2];
		  payloadLengthData = new long[input.length];
		  ccIdData = new long[input.length];
		  inputPosition = new long[input.length];
		  arrivalTimeAtPosition = new long[input.length];
		  blockedPosition = new long[input.length];
		  blockedOpenCount = new int[input.length];
		  blockedLen = new int[input.length];
		  blockedState = new int[input.length];
		  
		  runningHeaderBytes = new int[input.length];
		  			  
		  trieReader = new TrieParserReader(4);//max fields we support capturing.
		  
		  int x;
		  
		  //HTTP/1.1 200 OK
		  //revision  status# statusString\r\n
		  //headers
		  
	      ///////////////////////////
	      //Load the supported HTTP revisions
	      ///////////////////////////
	      revisionMap = new TrieParser(256,true); //TODO: set switch to turn on off the deep check skip
	      HTTPRevision[] revs = httpSpec.supportedHTTPRevisions.getEnumConstants();
	      x = revs.length;               
	      while (--x >= 0) {
	    	   //TODO: since most responses are 200 would this run faster if we hard coded that as part of the tree??
	            revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\r\n", revs[x].ordinal());
	            revisionMap.setUTF8Value(revs[x].getKey(), " %u %b\n", revs[x].ordinal());    //\n must be last because we prefer to have it pick \r\n
	      }

	    }

	int lastUser;
	int lastState;
	int lastCount;
	int responseCount = 0;
	
	void storeState(int state, int user) {
		if (state==lastState && lastUser == user) {
			lastCount++;
		} else {
			lastCount = 0;
		}
		lastState = state;
		lastUser = user;
	}
		
	@Override
	public void run() {
		
		int foundWork; //keep going until we make a pass and there is no work.
			

		do {		
			foundWork = 0;
			
			
			int i = input.length;
			while (--i>=0) {
			

				final int memoIdx = i<<2; //base index is i  * 4;
				final int posIdx   = memoIdx;
				final int lenIdx   = memoIdx+1;
				final int stateIdx = memoIdx+2;
				
				Pipe<NetResponseSchema> targetPipe = null;
				long ccId = 0;
				
				Pipe<NetPayloadSchema> localInputPipe = input[i];
		
				
				/////////////////////////////////////////////////////////////
				//ensure we have the right backing array, and mask (no position change)
				/////////////////////////////////////////////////////////////
				TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);

				TrieParserReader.parseSetup(
						     trieReader,
						     Pipe.blob(localInputPipe),
						     trieReader.sourcePos,
						     trieReader.sourceLen,
						     Pipe.blobMask(localInputPipe));							

//				TrieParserReader.parseSetup(
//					     trieReader,
//					     Pipe.blob(localInputPipe),
//					     0,
//					     0,
//					     Pipe.blobMask(localInputPipe));	
				
				HTTPClientConnection cc = null;
					
				int len1 = positionMemoData[lenIdx];
				int len2 = localInputPipe.maxVarLen;
				int len3 = Pipe.blobMask(localInputPipe);
				
				if (Pipe.hasContentToRead(localInputPipe)  
					//	&& 	((positionMemoData[lenIdx]+localInputPipe.maxVarLen+Pipe.releasePendingByteCount(localInputPipe)) < (Pipe.blobMask(localInputPipe) ) ) 
						 //TOOD: this second condition above should NOT be required but under heavy load this spins and never comes back..
						) {
					
					////////////////
					//before taking the data
					//ensure that it can be consumed 
					///////////////
					ccId = Pipe.peekLong(localInputPipe, 1);
					cc = (HTTPClientConnection)ccm.connectionForSessionId(ccId, true);
				
					if (null != cc) {
						//do not process if an active write for a different pipe is in process
						if (     (i != outputOwner[(int)cc.readDestinationRouteId()]) 
								&&  (-1 != outputOwner[(int)cc.readDestinationRouteId()])) {
							//move to the next pipe so we get it done.
							continue;
						}
						outputOwner[(int)cc.readDestinationRouteId()] = i;
					}

					boolean isClosed = ((null==cc) || (!cc.isValid()));
					if (isClosed) {	
						//this connection has been closed but before the
						//message is consumed we must make sure there is room
						//to write the close message.
						
						if (null!=cc && !Pipe.hasRoomForWrite(output[(int)cc.readDestinationRouteId()])) {
							continue;///can not process this close right now, no room.							
						}
					}

					
					//////////////////////////////
					//we have new data to consume
					//////////////////////////////		
					int msgIdx = Pipe.takeMsgIdx(localInputPipe);
					if (msgIdx<0) {
						throw new UnsupportedOperationException("no support for shutdown");
					}
					
					//TODO how many ccID connections are from this pipe?
					//     we must have N outgoing pipes from response or we can have a hang!!
					//     URGENT design change required...
					
					boolean ok = ccId == Pipe.takeLong(localInputPipe);
					assert(ok) : "Internal error";
					
					long arrivalTime = Pipe.takeLong(localInputPipe);
					//if already set do not set again, we want the leading edge of the data arrival.
					if (arrivalTimeAtPosition[i]<=0) {
						arrivalTimeAtPosition[i] = arrivalTime;
					}
					
					inputPosition[i] = Pipe.takeLong(localInputPipe);										
					ccIdData[i] = ccId;
		
					if (isClosed) {				
						
						int meta = Pipe.takeRingByteMetaData(localInputPipe);
						int len  = Pipe.takeRingByteLen(localInputPipe);
						int mask = blobMask(localInputPipe);	
						int pos = bytePosition(meta, localInputPipe, len)&mask;     		
						byte[] backing = byteBackingArray(meta, localInputPipe);
						
						logger.trace("closed connection detected");
						if (null != cc) {
							//publish closed to notify those down stream
							targetPipe = output[(int)cc.readDestinationRouteId()];
							
							Pipe.presumeRoomForWrite(targetPipe);
							int size = Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CLOSED_10);
							Pipe.addUTF8(cc.host, targetPipe);
							Pipe.addIntValue(cc.port, targetPipe);
							Pipe.confirmLowLevelWrite(targetPipe, size);
							Pipe.publishWrites(targetPipe);
							
							//data from the closed message...
							//TODO: need to refactor and push this message down stream to callers..
							StringBuilder closedMessage = new StringBuilder();
							Appendables.appendUTF8(closedMessage, backing, pos, len, mask);
							logger.error("closed response:\n{}\n<END OF ERROR>",closedMessage);
							
						}

						if (showTossedData) {
							
							StringBuilder builder = new StringBuilder();
							Appendables.appendUTF8(builder, backing, pos, len, mask);
							
							logger.warn("server closed connection and returned\n{}",builder);
							
						}
						Pipe.confirmLowLevelRead(localInputPipe, Pipe.sizeOf(localInputPipe, msgIdx)); 		
						Pipe.releaseReadLock(localInputPipe);
						positionMemoData[memoIdx+1] = 0;//wipe out existing data
						
						//drain all the data on this input pipe
						Pipe.publishBlobWorkingTailPosition(localInputPipe, Pipe.getWorkingBlobHeadPosition(localInputPipe));
						Pipe.publishWorkingTailPosition(localInputPipe, Pipe.workingHeadPosition(localInputPipe));
						continue;
					}
				
					targetPipe = output[(int)cc.readDestinationRouteId()];					
	
					//append the new data
					int meta = Pipe.takeRingByteMetaData(localInputPipe);
					int len = Pipe.takeRingByteLen(localInputPipe);
					int pos = Pipe.bytePosition(meta, localInputPipe, len);
					int mask = Pipe.blobMask(localInputPipe);
					
					///////////////////////////////////
					//logger.info("parse new data of {} for connection {}",len,cc.getId());
					//////////////////////////////////
					
					if (showData) {
						Appendables.appendUTF8(System.out /*capturedContent*/, Pipe.blob(localInputPipe), pos, len, mask);
					}
					///////////////////////////////////////////
					/////////////////////////////////////////
					
					if (positionMemoData[lenIdx]==0) {
						positionMemoData[posIdx] = pos;						
						positionMemoData[lenIdx] = len;
					} else {				
						positionMemoData[lenIdx] += len;
					}
					
					assert(positionMemoData[lenIdx]<Pipe.blobMask(localInputPipe)) : "error adding "+len+" total was "+positionMemoData[lenIdx]+" and should be < "+localInputPipe.blobMask(localInputPipe);
					

					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					//System.err.println(positionMemoData[lenIdx]+" vs "+pipe.byteMask);
					
				//	logger.info("reading in new data up to "+Pipe.getWorkingTailPosition(pipe)+" has "+positionMemoData[lenIdx]+" mask "+Pipe.blobMask(pipe));
					
					Pipe.confirmLowLevelRead(localInputPipe, Pipe.sizeOf(localInputPipe, msgIdx));   //release of read does not happen until the bytes are consumed...
					
					//WARNING: moving next without releasing lock prevents new data from arriving until after we have consumed everything.
					//  
					Pipe.readNextWithoutReleasingReadLock(localInputPipe);	
							
					if (-1==inputPosition[i]) {
						//this may or may not be the end of a complete message, must hold it just in case it is.
						inputPosition[i] = Pipe.getWorkingTailPosition(localInputPipe);//working tail is the right tested value
					}
					assert(inputPosition[i]!=-1);
					
					if 	(positionMemoData[lenIdx] >= Pipe.blobMask(localInputPipe) ) {
						logger.info("NEW CORRUPT DATA ERROR, response is not keeping up with the data "+positionMemoData[lenIdx]+" added "+len+" which should be < "+localInputPipe.maxVarLen);
						cc.close();
						return;
					}
					
					
				} else {
		
					
					TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
					
					if (trieReader.sourceLen==0 &&        //we have no old data
						0==positionMemoData[stateIdx]) {  //our state is back to step 0 looking for new data
						//We have no data in the local buffer and 
						//We have no data on this pipe so go check the next one.
						if (Pipe.contentRemaining(localInputPipe)>(localInputPipe.slabMask*.75)) {
							logger.warn("Can not read content because old data has not been released.");
						}
						continue;
						
					} else {
						//else use the data we have since no new data came in.
						
						ccId = ccIdData[i];
						cc = (HTTPClientConnection)ccm.connectionForSessionId(ccId);					
						if (null==cc) {	//skip data the connection was closed	
							ccm.releaseResponsePipeLineIdx(ccId);
							
							TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
							TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
							//System.err.println("yyy");
							continue;
						}
						
						//we have data which must be parsed and we know the output pipe, eg the connection was not closed						
						//convert long id to the pipe index.
						
						targetPipe = output[(int)cc.readDestinationRouteId()];
												
												
						///////////////////////
						//the fastest code is the code which is never run
						//do not parse again if nothing has changed
						////////////////////////
						final long headPos = Pipe.headPosition(localInputPipe);
						
						//TODO: this same approach should be used in server
						if (blockedPosition[i] == headPos 
							&& blockedLen[i] == trieReader.sourceLen) {
												
							if (blockedOpenCount[i]==0 
								&& Pipe.hasRoomForWrite(targetPipe) 
								&& blockedState[i] == positionMemoData[stateIdx] ) {								
								//we have the same data but we do have room for write so continue to try parse again
								//blockedPosition[i] = 0;	
								blockedOpenCount[i]++;
							} else {
								//System.err.println("zzz");
								continue;// do not parse again since nothing has changed	
							}
						} else {
							//did not eq last so we have more data and should attempt parse again
							blockedPosition[i] = headPos;
							blockedOpenCount[i] = 0;
							blockedLen[i] = trieReader.sourceLen;
							blockedState[i] =  positionMemoData[stateIdx];							
						}						
					}
				}

				////////////
				////////////
				//do not process if an active write is in process
				if (     (i != outputOwner[(int)cc.readDestinationRouteId()]) 
				    &&  (-1 != outputOwner[(int)cc.readDestinationRouteId()])) {
					//multiple connections write to the same pipe, this keeps them organized.
					//move to the next one because this one is blocked
					//System.err.println("qqq");
					continue;
				}
				outputOwner[(int)cc.readDestinationRouteId()] = i;
				////////////
				////////////
				
				if (Pipe.hasContentToRead(localInputPipe) || positionMemoData[lenIdx]>localInputPipe.maxVarLen   ) {
					foundWork++;//do not leave if we are backed up
				}

				int state = positionMemoData[stateIdx];

				if (state==0) {
					assert (!Pipe.isInBlobFieldWrite(targetPipe)) : 
						   "for starting state expected pipe to NOT be in blob write";
				}
			
					//TODO: may be faster with if rather than switch.

				//System.err.println("on state "+state);
				
				 switch (state) {
					case 0:////HTTP/1.1 200 OK              FIRST LINE REVISION AND STATUS NUMBER
						
						if (null==targetPipe || !Pipe.hasRoomForWrite(targetPipe)) { 
							break; //critical check
						}
						
						int startingLength1 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
						
						if (startingLength1<(revisionMap.shortestKnown()+1)) {
							break;
						}		
					
						final int revisionId = (int)TrieParserReader.parseNext(trieReader, revisionMap);
						if (revisionId>=0) {
													
							payloadLengthData[i] = 0;//clear payload length rules, to be populated by headers
														
							//because we have started written the response we MUST do extra cleanup later.
							Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_RESPONSE_101);
							Pipe.addLongValue(ccId, targetPipe); // NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1, ccId);
						
							Pipe.addIntValue(ServerCoordinator.BEGIN_RESPONSE_MASK, targetPipe);//flags, init to zero, will set later if required

							positionMemoData[stateIdx]= ++state;//state change is key
							DataOutputBlobWriter<NetResponseSchema> openOutputStream = Pipe.openOutputStream(targetPipe);
							
							DataOutputBlobWriter.tryClearIntBackData(openOutputStream, cc.totalSizeOfIndexes()); 
	
							//NOTE: this is always first and not indexed...
							TrieParserReader.writeCapturedShort(trieReader, 0, openOutputStream); //status code	
										
							runningHeaderBytes[i] = startingLength1 - trieReader.sourceLen;
	
												
						} else {
							assert(trieReader.sourceLen <= trieReader.sourceMask) : "ERROR the source length is larger than the backing array";
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
							if (trieReader.sourceLen < (revisionMap.longestKnown()+1)) {
								break;//not an error just needs more data.
							} else {
								//TODO: rollback the previous message write since it can not be compeleted? or just trucate it?? TODO: urgent error support
								
								//logger.info("error trieReader pos {} len {} ", trieReader.sourcePos,trieReader.sourceLen);
								
								reportCorruptStream("HTTP revision",cc);

								///////////////////////////////////
								//server is behaving badly so shut the connection
								//////////////////////////////////
								cc.close();		
								cc.clearPoolReservation();
								ccm.releaseResponsePipeLineIdx(cc.id);
								
								TrieParserReader.parseSkip(trieReader, trieReader.sourceLen);
								TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
																
							}
		
							break;
						}
						
						assert(positionMemoData[stateIdx]==1);
					case 1: ///////// HEADERS
						
						//TODO: look up the right headerMap...
						//      these are based on the headers that the client caller requests
						//      these headers should be defined in the ClientHostPortInstance object.
						
						
						//this writer was opened when we parsed the first line, now we are appending to it.
						DataOutputBlobWriter<NetResponseSchema> writer = Pipe.outputStream(targetPipe);
						
						final int startingPosition = writer.absolutePosition();
							
						
						long headerToken=0;
						//stay here and read all the headers if possible
						do {
							int startingLength = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	 //TODO = save position is wrong if we continue???
													
							//TODO: based on record/route ID look up the right header map

							headerToken = TrieParserReader.parseNext(trieReader, cc.headerParser());	
						
							assert(headerToken==-1 || headerToken>=(Integer.MAX_VALUE-2)) : "bad token "+headerToken;
							
							//TODO: get the associated object with this field
							//      remove the old constants for check
							//      get map from the host pipe instance
							// 
									
							if (headerToken != -1) {											
								
								if (HTTPSpecification.END_OF_HEADER_ID != headerToken) {	
									
									headerProcessing(i, writer, headerToken, cc);
									
									//do not change state we want to come back here.									
								} else {
									//logger.trace("end of headers");
									state = endOfHeaderProcessing(i, stateIdx, writer);
																											
									//logger.trace("finished reading header now going to state {}",state);
								
									if (3==state) {
										//release all header bytes, we will do each chunk on its own.
										assert(runningHeaderBytes[i]>0);								
										Pipe.releasePendingAsReadLock(localInputPipe, runningHeaderBytes[i]); 
										runningHeaderBytes[i] = 0; 
									}
									//only case where state is not 1 so we must call save all others will call when while loops back to top.
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); 

									//logger.info("payload position {} {}  {}",Long.toBinaryString(cc.payloadToken),cc.payloadToken,writer.position());
									//NOTE: payload index position is always zero 
									DataOutputBlobWriter.setIntBackData(writer, writer.position(), 0);
									
									
								}								
								
								int consumed = startingLength - trieReader.sourceLen;							
								runningHeaderBytes[i] += consumed;
								
							} 
						} while ((headerToken != -1) && state==1);
						
						if (headerToken == -1) {
							
							writer.absolutePosition(startingPosition);
							
							TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
							
							if (trieReader.sourceLen<MAX_VALID_HEADER) {		
								break;//not an error just needs more data.
							} else {
							    
								reportCorruptStream2(cc);
								
								//TODO: bad client, disconnect??  finish partial message out!!!
								
							}
							
							assert(trieReader.sourceLen == Pipe.releasePendingByteCount(localInputPipe)) : trieReader.sourceLen+" != "+Pipe.releasePendingByteCount(localInputPipe);
							assert(positionMemoData[i<<2] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[i<<2]+" != "+Pipe.releasePendingByteCount(input[i]);
				    		break;
						}
						
				
					case 2: //PAYLOAD READING WITH LENGTH
							//if we can not release then do not finish.
							if (!Pipe.hasRoomForWrite(releasePipe)) {
								break;
							}
							if (2==state) {
								//TODO: should we do the JSON Extraction here??
								//      no we need to inject another stage between for that.
								
																
								long lengthRemaining = payloadLengthData[i];
													
		//						logger.info("source position {} state {} length remaining to copy {} source len ",trieReader.sourcePos,state,lengthRemaining,trieReader.sourceLen);
								
								final DataOutputBlobWriter<NetResponseSchema> writer2 = Pipe.outputStream(targetPipe);
							
								if (lengthRemaining>0 && trieReader.sourceLen>0) {
													
									//length is not written since this may accumulate and the full field provides the length
									final int consumed = TrieParserReader.parseCopy(trieReader,
											                          Math.min(lengthRemaining,
											                        		   DataOutputBlobWriter.lastBackPositionOfIndex(writer2)),
											                          writer2);
									lengthRemaining -= consumed;
									
									//NOTE: if the target field is full then we must close this one and open a new
									//      continuation.
										
									//TODO: add and fix this feature...
									if (lengthRemaining>0) {
										logger.info("incomplete feature needs to be finshed.");
			
										DataOutputBlobWriter.commitBackData(writer2, cc.getStructureId());
									
										
										int len = writer2.closeLowLevelField();
										//logger.trace("conform low level write of len {} ",len);
										Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
										Pipe.publishWrites(targetPipe);
										//DO NOT consume since we still need it
										
										if (!Pipe.hasRoomForWrite(targetPipe)) { //TODO: fix this case
											logger.info("ERROR MUST TRY LATER AFTER CONSUME???");
											
											//TODO: need to jump to new state adding the continuation message
											//      then we can ext and return at this point.
											
										}
										
										Pipe.presumeRoomForWrite(targetPipe);
										
										//logger.trace("begin new continuation");
										
										//prep new message for next time.
										Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
										Pipe.addLongValue(ccId, targetPipe); //same ccId as before
										Pipe.addIntValue(0, targetPipe); //flags							
										DataOutputBlobWriter.openField(writer2);
									}								
									
		//							logger.info("consumed {} source position {} state {} ",consumed, trieReader.sourcePos,state);
									
									assert(runningHeaderBytes[i]>0);
									runningHeaderBytes[i] += consumed;		
									
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx); //TODO = save position is wrong if we continue???
								}
								payloadLengthData[i] = lengthRemaining;
								
								if (0 == lengthRemaining) {
		//							logger.info("lenRem 0 source position {} state {} ",trieReader.sourcePos,state);
									
									Pipe.releasePendingAsReadLock(localInputPipe, runningHeaderBytes[i]); 
					
									DataOutputBlobWriter.commitBackData(writer2,  cc.getStructureId());
																	
									int length = writer2.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
									//logger.info("length of full message written {} ",length);
									
									positionMemoData[stateIdx] = state = 5;
									
									//NOTE: go back and set the bit for end of data, 1 for msgId, 2 for connection Id	
									Pipe.orIntValue(ServerCoordinator.END_RESPONSE_MASK, 
											         targetPipe, 
											         Pipe.lastConfirmedWritePosition(targetPipe)+(0xFF&NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5));
									
									Pipe.confirmLowLevelWrite(targetPipe, SIZE_OF_MSG_RESPONSE);
									int totalConsumed = Pipe.publishWrites(targetPipe);	
									//logger.trace("total consumed msg response write {} internal field {} varlen {} ",totalConsumed, length, targetPipe.maxVarLen);					
									//clear the usage of this pipe for use again by other connections
									outputOwner[(int)cc.readDestinationRouteId()] = -1; 
									long routeId = cc.consumeDestinationRouteId();////////WE ARE ALL DONE WITH THIS RESPONSE////////////

									//expecting H to be the next valid char 
	//								assert(trieReader.sourceLen<=0 || input[i].blobRing[input[i].blobMask&trieReader.sourcePos]=='H') :"bad next value of "+(int)input[i].blobRing[input[i].blobMask&trieReader.sourcePos];
									
									
									assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";

									foundWork += finishAndRelease(i, stateIdx, localInputPipe, cc, 0); 
									state = positionMemoData[stateIdx];
																
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
																		
									break;
								} else {
									
									assert(lengthRemaining>0);
									break;//we have no data and need more.
								}
							}
							if (3!=state) {
								break;
							}
	
					case 3: //PAYLOAD READING WITH CHUNKS	
						
							//if we can not release then do not finish.
							if (!Pipe.hasRoomForWrite(releasePipe)) {
								break;
							}

					  	    long chunkRemaining = payloadLengthData[i];

							DataOutputBlobWriter<NetResponseSchema> writer3 = Pipe.outputStream(targetPipe);
							do {
								if (0==chunkRemaining) {
									
									int startingLength3 = TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);	
								
									if ((int)TrieParserReader.parseNext(trieReader, HTTPUtil.chunkMap) < 0) {
										//FORMAL ERROR, we can never support a chunk bigger than a 64 bit number which is 16 chars in hex.
										if (trieReader.sourceLen>16) {
											parseErrorWhileChunking(memoIdx, localInputPipe, trieReader.sourcePos);
										}
										//logger.info("need chunk data");
										return;	//not enough data yet to parse try again later
									}
								
									chunkRemaining = TrieParserReader.capturedLongField(trieReader,0);
									//logger.info("reading a fresh chunk of size {}",chunkRemaining);
									
									if (0==chunkRemaining) {
										
										//TODO: Must add parse support for trailing headers!, 
										//       this is a hack for now. needs its own case...
										//TODO: remove this spin
										do {
											headerToken = TrieParserReader.parseNext(trieReader, cc.headerParser());
											
										} while (HTTPSpecification.END_OF_HEADER_ID!=headerToken && -1!=headerToken);
																				
										
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);											
										int consumed = startingLength3 - trieReader.sourceLen;
										
										//TODO: not sure why chunk requires  extra bytes
										//  we know that the trieReader is fully consumed since 
										//  following message is read correctly.
																				
										Pipe.releasePendingAsReadLock(localInputPipe, consumed);
										int b = 2;//Pipe.releasePendingByteCount(localInputPipe);
										//System.err.println("missing count"+b);
										Pipe.releasePendingAsReadLock(localInputPipe, b);
										
										DataOutputBlobWriter.commitBackData(writer3, cc.getStructureId());
										
										int len = writer3.closeLowLevelField(); //NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3
										//logger.info("nothing remaing in this chunk moving to state 5");
										positionMemoData[stateIdx] = state = 5;
										
										//logger.info("Detected last chunk so send the flag showing we are done\n length {}",len);
																				
										Pipe.orIntValue(ServerCoordinator.END_RESPONSE_MASK, 
												        targetPipe, 
													    Pipe.lastConfirmedWritePosition(targetPipe)+(0xFF&NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5));
										
										Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
										Pipe.publishWrites(targetPipe);	
										
										//clear the usage of this pipe for use again by other connections
										outputOwner[(int)cc.readDestinationRouteId()] = -1; 
										long routeId = cc.consumeDestinationRouteId();////////WE ARE ALL DONE WITH THIS RESPONSE////////////

										
										assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";

				                    	foundWork += finishAndRelease(i, stateIdx, localInputPipe, cc, 0); 
										
										break;
									} else {
											
										payloadLengthData[i] = chunkRemaining;										
										int consumed = startingLength3 - trieReader.sourceLen;
										Pipe.releasePendingAsReadLock(localInputPipe, consumed);
										TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
										
										//ensure we do not override the indexes
										if ((DataOutputBlobWriter.lastBackPositionOfIndex(writer3)-(writer3.length() + chunkRemaining))>0) {
											DataOutputBlobWriter.commitBackData(writer3, cc.getStructureId());
											
											int len = writer3.closeLowLevelField();
											//logger.trace("conform low level write of len {} ",len);
											Pipe.confirmLowLevelWrite(targetPipe); //uses auto size since we do not know type here
											Pipe.publishWrites(targetPipe);
																						
											//DO NOT consume route id we will still need it.
											//state is already 3 so leave it there
											if (Pipe.hasRoomForWrite(targetPipe)) {
												//logger.trace("begin new continuation");
												
												//prep new message for next time.
												Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
												Pipe.addLongValue(ccId, targetPipe); //same ccId as before
												Pipe.addIntValue(0, targetPipe); //flags
												DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
												DataOutputBlobWriter.openField(writer1);	
												
											} else {
												//switch to 4 until the outgoing pipe is cleared
												//then we come back to 3
												positionMemoData[stateIdx] = state = 4;												
											}
											//logger.info("start collecting new continuation");
											
											break;
										}
									}
								}				
								
								
								////////
								//normal copy of data for chunk
								////////
								
								if (chunkRemaining>0) {
									int initValue = trieReader.sourceLen;
									//can do some but not the last byte unless we have the 2 following bytes a well. must have teh \r\n in addition to the remaining byte count								
									long maxToCopy = chunkRemaining;
									if (trieReader.sourceLen==chunkRemaining || (trieReader.sourceLen-1)==chunkRemaining) {
										maxToCopy = 0;
									}
									
								//	System.err.println("at copy postion is "+trieReader.sourcePos);
								//	TrieParserReader.debugAsUTF8(trieReader, System.err, 10, false); //we know the data is here buy why not on the stream?
									
									int temp3 = TrieParserReader.parseCopy(trieReader, maxToCopy, writer3);
									chunkRemaining -= temp3;
									
									assert(chunkRemaining>=0);
									
									if (chunkRemaining==0) {
										payloadLengthData[i] = 0; //signal that we need more, in case we exit block
										//System.err.println("ZZ copied fully over "+temp3);
										//NOTE: assert of these 2 bytes would be a good idea right here.
										int skipped = TrieParserReader.parseSkip(trieReader, 2); //skip \r\n which appears on the end of every chunk
										if (skipped!=2) {//TODO: convert to assert.
											throw new UnsupportedOperationException("Can not skip 2 bytes they are not here yet.");
										}
										temp3+=2;
									} else {
										//System.err.println("ZZ copied partial over "+temp3);
										
										payloadLengthData[i] = chunkRemaining;
									}
									TrieParserReader.savePositionMemo(trieReader, positionMemoData, memoIdx);
				
									assert((initValue-trieReader.sourceLen)==temp3);
									
									Pipe.releasePendingAsReadLock(localInputPipe, temp3);
								}
								
													
							} while (0 == chunkRemaining);
				
							break;
					case 4:
						//if there is no room then stay on case 4 and wait
						if (Pipe.hasRoomForWrite(targetPipe)) {
						
							//logger.trace("begin new continuation");
							
							//prep new message for next time.
							Pipe.addMsgIdx(targetPipe, NetResponseSchema.MSG_CONTINUATION_102);
							Pipe.addLongValue(ccId, targetPipe); //same ccId as before
							Pipe.addIntValue(0, targetPipe); //flags
							DataOutputBlobWriter<NetResponseSchema> writer1 = Pipe.outputStream(targetPipe);							
							DataOutputBlobWriter.openField(writer1);
							
							//go back and continue roll up the data
							positionMemoData[stateIdx] = state = 3;
							//these two lines clear the blocks so we can come back to 4 when there is data..
							blockedOpenCount[i] = 0;
							blockedState[i] = positionMemoData[stateIdx];
									
						} 
						break;
					case 5: //END SEND ACK
						logger.info("source position {} source length {} state {} ",trieReader.sourcePos,trieReader.sourceLen,state);
						
											
						assert (!Pipe.isInBlobFieldWrite(targetPipe)) : "for starting state expected pipe to NOT be in blob write";

					    foundWork += finishAndRelease(i, stateIdx, localInputPipe, cc, 0);
				
					    assert(positionMemoData[(i<<2)+1] == Pipe.releasePendingByteCount(input[i])) : positionMemoData[(i<<2)+1]+" != "+Pipe.releasePendingByteCount(input[i]);
					    
						break;	
												
				}	

				 
			}
		} while(foundWork>0);//hasDataToParse()); //stay when very busy
		
	}



	@Override
	public void shutdown() {
	}

	private void parseErrorWhileChunking(final int memoIdx, Pipe<NetPayloadSchema> pipe, int readingPos) {
		System.err.println("SHUTING DOWN NOW: chunk ID is TOO long starting at "+readingPos+" data remaining "+trieReader.sourceLen);
		
		ByteArrayOutputStream ist = new ByteArrayOutputStream();
		trieReader.debugAsUTF8(trieReader, new PrintStream(ist), 100,false);
		byte[] data = ist.toByteArray();
		System.err.println(new String(data));
		
		System.err.println("bad data pipe is:"+pipe);
		trieReader.debug(); //failure position is AT the mask??
		
		TrieParserReader.loadPositionMemo(trieReader, positionMemoData, memoIdx);
		int chunkId3 = (int)TrieParserReader.parseNext(trieReader, HTTPUtil.chunkMap);
		System.err.println("parsed value was "+chunkId3);
		
		requestShutdown();
	}

	private void reportCorruptStream2(ClientConnection cc) {
		StringBuilder builder = new StringBuilder();
		TrieParserReader.debugAsUTF8(trieReader, builder, revisionMap.longestKnown()*2);
		logger.warn("{} looking for header field but found:\n{}\n\n",cc.id,builder);
	}

	private void reportCorruptStream(String label, ClientConnection cc) {
		StringBuilder builder = new StringBuilder();
		TrieParserReader.debugAsUTF8(trieReader, builder, revisionMap.longestKnown()*2,false);
		logger.warn("{} looking for {} but found:\n{}\n\n",cc.id,label,builder);
	}

	private int endOfHeaderProcessing(int i, final int stateIdx, DataOutputBlobWriter<NetResponseSchema> writer) {
		int state;
		//all done with header move on to body
		writer.writeShort((short)-1); //END OF HEADER FIELDS 		
		
		//Now write header message, we know there is room because we checked before starting.
						    
		if (payloadLengthData[i] == -1) {
			positionMemoData[stateIdx]= state= 3;	
			payloadLengthData[i] = 0;//starting chunk size.			

		} else {
			positionMemoData[stateIdx]= state= 2;
		}
		return state;
	}

	private void headerProcessing(int i, 
			                     DataOutputBlobWriter<NetResponseSchema> writer, 
			                     long headerToken, HTTPClientConnection cc) {
		
		if (HTTPSpecification.UNKNOWN_HEADER_ID == headerToken) {
			assert(confirmCoreHeadersSupported());
			boolean verboseShowHeaders = false;
			if (verboseShowHeaders) {
				reportUnsupportedHeader(); //ignore since we did not want this	
			}
		} else {
			//NOTE: any replacement headers must hold the same ordinal positions!!
			//NOTE: any header fields must hold the associated enum 
			HTTPHeader header = cc.associatedFieldObject(headerToken);
			
			//logger.info("header found {} ",header);
			
			if (null!=header) {
				int writePosition = writer.position();				

				if (HTTPHeaderDefaults.CONTENT_LENGTH.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.CONTENT_LENGTH.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONTENT_LENGTH does not match";
					
					long length = TrieParserReader.capturedLongField(trieReader, 0);
					//logger.info("content length of  {} found in headers ",length);
					
					writer.writePackedLong(length);
					
					if (-1 != payloadLengthData[i]) {
						payloadLengthData[i] = length;
						lastPayloadSize = length;						
					}			
					
				} else if (HTTPHeaderDefaults.CONTENT_TYPE.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.CONTENT_TYPE.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, CONTENT_TYPE does not match";
								
					int type = (int)TrieParserReader.capturedFieldQuery(trieReader, 0, httpSpec.contentTypeTrieBuilder());
	
					//logger.trace("detected content type and set value to {} ",type);
					
					lastMessageType = type;			
					writer.writeShort((short)type);
									
				} else if (HTTPHeaderDefaults.TRANSFER_ENCODING.ordinal()==header.ordinal()) {
					assert(Arrays.equals(HTTPHeaderDefaults.TRANSFER_ENCODING.rootBytes(),header.rootBytes())) : "Custom enums must share same ordinal positions, TRANSFER_ENCODING does not match";
								
					writer.writeBoolean(true); //true for chunked
					payloadLengthData[i] = -1; //marked as chunking	
				
				} else {
					//normal processing
					TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer);
				}
				
				
				final long id1 = headerToken;
				
				//logger.info("aaa struct {} wrote position {} for header {} at location {} in pipe {}",
				//		Integer.toHexString((int)(id1>>>BStructSchema.STRUCT_OFFSET)),
				//		writePosition, header, BStructSchema.FIELD_MASK & (int)headerToken, writer.getPipe().id);
				
				DataOutputBlobWriter.setIntBackData(writer, writePosition, StructRegistry.FIELD_MASK & (int)headerToken);
			}
			
		}
				 
	}

	private BloomFilter filter;
	
	private void reportUnsupportedHeader() {
		StringBuilder headerName = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, headerName); //in TRIE if we have any exact matches that run short must no pick anything.
		
		if (null==filter) {
			filter = new BloomFilter(10000, .00001); //32K
		}
		
		if (filter.mayContain(headerName)) {
			return;//do not report since we have already done so or we are overloaded with noise
		} else {
			filter.addValue(headerName);
		}
				
		StringBuilder headerValue = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 1, headerValue); //in TRIE if we have any exact matches that run short must no pick anything.
							
		logger.info("WARNING unsupported header found: {}: {}",headerName, headerValue);
		///logger.trace("length avail when parsed {}",len);
	}
	
	private boolean confirmCoreHeadersSupported() {
		StringBuilder headerName = new StringBuilder();					
		TrieParserReader.capturedFieldBytesAsUTF8(trieReader, 0, headerName); //in TRIE if we have any exact matches that run short must no pick anything.
		headerName.append(": ");
		String header = headerName.toString();
				
		if (   HTTPHeaderDefaults.CONTENT_LENGTH.writingRoot().equals(header) 
			|| HTTPHeaderDefaults.CONTENT_TYPE.writingRoot().equals(header) 
			|| HTTPHeaderDefaults.TRANSFER_ENCODING.writingRoot().equals(header) 
				) {
			logger.warn("Did not recognize header {}", header);
			return false;
		}
		
		return true;
	}

	private int finishAndRelease(int i, final int stateIdx, Pipe<NetPayloadSchema> pipe, ClientConnection cc, int nextState) {
		

		assert(positionMemoData[stateIdx]>=5);
		
		int foundWork = 0;
		//only ack when all the data held has been consumed.
		if (trieReader.sourceLen<=0 &&
		    Pipe.contentRemaining(pipe)==0) {	//added second rule to minimize release messages.
			
			foundWork = sendRelease(stateIdx, cc.id, inputPosition, i);
			
			//may return without setting ack because pipe is full.	
			if (positionMemoData[stateIdx] != 0) {
				logger.info("not finished {})",cc.id);
			}

		} else {
			
			foundWork = 1;						
			positionMemoData[stateIdx] = nextState;
		}
		//records the leading edge of arrival time.
		long temp = arrivalTimeAtPosition[i];
		if (0 != temp) {
			cc.recordArrivalTime(temp);
			arrivalTimeAtPosition[i] = 0;
		}
		
		return foundWork;
	}

	private int sendRelease(final int stateIdx, long ccId, long[] position, int i) {

		Pipe.presumeRoomForWrite(releasePipe);
		
		int size = Pipe.addMsgIdx(releasePipe, ReleaseSchema.MSG_RELEASE_100);
		Pipe.addLongValue(ccId, releasePipe);
		Pipe.addLongValue(position[i], releasePipe);
		Pipe.confirmLowLevelWrite(releasePipe, size);
		Pipe.publishWrites(releasePipe);
		positionMemoData[stateIdx] = 0;
		
		position[i] = -1; 
		
		return 1;

	}

}
