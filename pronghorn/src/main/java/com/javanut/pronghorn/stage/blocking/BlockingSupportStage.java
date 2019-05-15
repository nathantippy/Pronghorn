package com.javanut.pronghorn.stage.blocking;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * Stage that allows for blocking calls, e.g. to make a call to a database and then wait
 * until a response is received.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
@Deprecated
public class BlockingSupportStage<T extends MessageSchema<T>, P extends MessageSchema<P>, Q extends MessageSchema<Q>> extends PronghornStage {

	private final Pipe<T> input;
	private final Pipe<P> output;
	private final Pipe<Q> timeout;
	private Choosable<T> chooser;
	private Blockable<T, P, Q>[] blockables;
	private Thread[] threads;
	private long[] times;
	private boolean[] needsWorkWaiting;
	private boolean[] completedWorkWaiting;
	private long timeoutNS;
	private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
	private Logger logger = LoggerFactory.getLogger(BlockingSupportStage.class);
	private final Pipe<BlockingWorkInProgressSchema> inProgress;
	private final UnchosenMessage<T> unchosen;
	
	/**
	 *
	 * @param graphManager
	 * @param input _in_ Input that will be released until ready
	 * @param output _out_ Pipe onto which the input will be released on
	 * @param timeout
	 * @param timeoutNS
	 * @param chooser
	 * @param blockables
	 */
	public BlockingSupportStage(GraphManager graphManager, Pipe<T> input, Pipe<P> output, Pipe<Q> timeout, long timeoutNS, Choosable<T> chooser, UnchosenMessage<T> unchosen, Blockable<T,P,Q> ... blockables) {
		this(graphManager,input,output,timeout,workPipe(blockables.length),timeoutNS, chooser,unchosen,blockables);
	}
	
	private static Pipe<BlockingWorkInProgressSchema> workPipe(int length) {
		return BlockingWorkInProgressSchema.instance.newPipe(length, 0);
	}

	public BlockingSupportStage(GraphManager graphManager, Pipe<T> input, Pipe<P> output, Pipe<Q> timeout, Pipe<BlockingWorkInProgressSchema> workload, long timeoutNS, Choosable<T> chooser, UnchosenMessage<T> unchosen, Blockable<T,P,Q> ... blockables) {
		super(graphManager, join(input, workload), join(output==timeout ? join(output) : join(output,timeout),workload));
		this.input = input;
		this.output = output;
		this.timeout = timeout;
		this.chooser = chooser;
		this.blockables = blockables;
		this.timeoutNS = timeoutNS;
		this.inProgress = workload;
		this.unchosen = unchosen;
		
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lightcoral", this);

	}
	
	@Override
	public void startup() {
		times = new long[blockables.length];
		needsWorkWaiting = new boolean[blockables.length];
		completedWorkWaiting = new boolean[blockables.length];
		
		threads = new Thread[blockables.length];
		ThreadGroup threadGroup = new ThreadGroup("BlockingGroup-"+stageId);
		int t = threads.length;
		while (--t >= 0) {
			Blockable<T, P, Q> blockable = blockables[t];
			threads[t] = new Thread(threadGroup,buildRunnable(t),blockable.name()+"-"+t,blockable.requestedStackSize());
			threads[t].start();
		}
	}
	
	private Runnable buildRunnable(final int instance) {
		
		final Blockable<T, P, Q> b = blockables[instance];

		return new Runnable() {

			@Override
			public void run() {
				synchronized(b) {
					while (!isShuttingDown.get()) {
						try {					
							needsWorkWaiting[instance] = true;
							//System.err.println("---------------- "+instance+" waiting for work");
							b.wait();

						} catch (InterruptedException e) {
						} finally {
							if (isShuttingDown.get()) {
								return;//exit now...
							}
							
						}
												
						try {					
							//needed for external timeout checking
							times[instance] = System.nanoTime();
							
							try {
								//logger.info("\n---running start {}",instance);
								b.run();
								//logger.info("\n---running stop {}",instance);
							} catch (Exception e) {
								//for SQL exceptions
								e.printStackTrace();
								if (!e.getClass().getSimpleName().contains("SQL")) {
									throw e;
								}
							}
							
							times[instance] = 0;//clear
							//logger.info("\n---completedWorkWaiting {}",instance);
							completedWorkWaiting[instance] = true;
							b.wait();
							
						} catch (InterruptedException ie) {
							b.timeout(timeout);
							completedWorkWaiting[instance] = false;
						} catch (Exception ie) {
							b.timeout(timeout);
							completedWorkWaiting[instance] = false;
						}				
						
					}
				}				
			}			
		};
	}

	@Override
	public void run() {
		
		//pick up as much new work as we can

		while (Pipe.hasContentToRead(input)) {
			int choice = chooser.choose(input);
			if (choice>=0) {					
				if (needsWorkWaiting[choice] && Pipe.hasRoomForWrite(inProgress)) {
					//logger.info("\n---selected choice {}",choice);
					
					Blockable<T,P,Q> b = blockables[choice];
					synchronized(b) {
						if (needsWorkWaiting[choice]) {
							if (b.begin(input)) {//returns true if new work was accepted
								//logger.info("\n---begin {}",choice);
								needsWorkWaiting[choice] = false;
								b.notify();
								
								int size = Pipe.addMsgIdx(inProgress, BlockingWorkInProgressSchema.MSG_INFLIGHT_1);
								Pipe.confirmLowLevelWrite(inProgress, size);
								Pipe.publishWrites(inProgress);
							}
						}
					} 
				}else {
					break;
				}
			} else {
				//logger.info("\n choice negative so sent to unchosen");
				//choice is negative so the waiting content must be sent to the unchosen
				unchosen.message(input);				
				break;
			}
		}
	
		
		//finish any complete jobs
		int j = completedWorkWaiting.length;
		while (--j>=0) {
			
			if (completedWorkWaiting[j] && Pipe.hasRoomForWrite(output)) {
				//logger.info("\n---found completed work waiting {}",j);
				Blockable<T,P,Q> b = blockables[j];
				synchronized(b) {
					if (completedWorkWaiting[j]) {
						//logger.info("\n---finish {}",j);
						b.finish(output);
						completedWorkWaiting[j] = false;
						b.notify();
						times[j] = 0;
						if (Pipe.hasContentToRead(inProgress)) {
							Pipe.skipNextFragment(inProgress);
						}
						
					}
				}
			}			
		}		
		
		//check for timeouts
		long now = System.nanoTime();
		int t = times.length;
		while (--t>=0) {
			long localTime = times[t];
			if (0!=localTime) {
				long duration = now - localTime;
				if (duration>timeoutNS && Pipe.hasRoomForWrite(timeout)) {
					logger.info("timeout task {}ns",duration);
					
					if (Pipe.hasContentToRead(inProgress)) {
						Pipe.skipNextFragment(inProgress);
					}
					threads[t].interrupt(); 
				}		
			}
		}	
		
		
	}

	@Override
	public void shutdown() {
		isShuttingDown.set(true);
		int t = times.length;
		while (--t>=0) {
			Blockable<T,P,Q> b = blockables[t];
			synchronized(b) {
				threads[t].interrupt();
			}	
		}
	}
	
}
