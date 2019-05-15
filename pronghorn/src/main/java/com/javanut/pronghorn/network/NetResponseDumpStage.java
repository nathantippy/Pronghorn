package com.javanut.pronghorn.network;

import java.io.IOException;

import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.http.HeaderUtil;
import com.javanut.pronghorn.network.schema.NetResponseSchema;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.util.Appendables;

/**
 * Dumps a NetResponseSchema onto an Appendable target.
 * @param <A>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class NetResponseDumpStage<A extends Appendable> extends PronghornStage {

	private final Pipe<NetResponseSchema> input;
	private final A target;
	private final HTTPSpecification<?, ?, ?, ?> httpSpec;

	/**
	 *
	 * @param graphManager
	 * @param input _in_ The net response input pipe.
	 * @param target The Appendable onto which the net response will be dumped.
	 * @param httpSpec
	 */
	public NetResponseDumpStage(GraphManager graphManager, 
			                    Pipe<NetResponseSchema> input, 
			                    A target, 
			                    HTTPSpecification<?, ?, ?, ?> httpSpec) {
		super(graphManager, input, NONE);
		this.input = input;
		this.target = target;
		this.httpSpec = httpSpec;
	}

	@Override
	public void run() {
		
		while(Pipe.hasContentToRead(input)) {
			
			int id = Pipe.takeMsgIdx(input);
			switch (id) {
				case NetResponseSchema.MSG_RESPONSE_101:
					{
						long connection = Pipe.takeLong(input);
						int userSessionId = Pipe.takeInt(input);
						int flags = Pipe.takeInt(input);
						 
						DataInputBlobReader<NetResponseSchema> stream = Pipe.openInputStream(input);
												
						int status = stream.readShort();
						System.out.println("status:"+status);
						
						int headerId = stream.readShort();
						
						while (-1 != headerId) { //end of headers will be marked with -1 value
							
							httpSpec.writeHeader(
									Appendables.appendValue(System.out, "", headerId, ": "), 
									headerId, stream);
							
							//read next
							headerId = stream.readShort();
							
						}
						//System.out.println("last short:"+headerId);
						
						try {
							DataInputBlobReader.readUTF(stream, stream.available(), target);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
						Pipe.releaseReadLock(input);
					}	
					
					break;
				case NetResponseSchema.MSG_CONTINUATION_102:
					{
						long connection = Pipe.takeLong(input);
						int session = Pipe.takeInt(input);
						int flags2 = Pipe.takeInt(input);
		            	 
						DataInputBlobReader<NetResponseSchema> stream = Pipe.inputStream(input);
						stream.openLowLevelAPIField();
						
						//NOTE: how do we know to remove the headers??
						stream.readUTF(target);
						
						Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
						Pipe.releaseReadLock(input);
					}
					
					break;
					
				case NetResponseSchema.MSG_CLOSED_10:
					
					Pipe.takeLong(input);
					Pipe.takeInt(input);
					
					Pipe.takeByteArrayMetaData(input);
					Pipe.takeByteArrayLength(input);
					
					Pipe.takeInt(input);
					
					Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
					Pipe.releaseReadLock(input);
					
					break;
				case -1:
					Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(input);
					requestShutdown();
					return;
			    default:
			        throw new UnsupportedOperationException("not yet implemented support for "+id);	 
			
			}
		
			
			
			
		}
		
		
	}

}
