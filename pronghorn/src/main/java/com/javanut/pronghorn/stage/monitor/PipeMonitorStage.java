package com.javanut.pronghorn.stage.monitor;

import static com.javanut.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * Stage which monitors pipes in real time.
 * This data is passed along for the telemetry.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class PipeMonitorStage extends PronghornStage {

	private final Pipe<?>[] observedPipe;
	private final Pipe<PipeMonitorSchema>[] notifyRingBuffer;
	private final GraphManager gm;
	private static final Logger logger = LoggerFactory.getLogger(PipeMonitorStage.class);
	private long dropped = 0;
	private final Pipe[] mapPipeIdToObservedPipe;
	
	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer _out_ observation pipes
	 * @param notifyRingBuffer _out_ notify pipes
	 */
	public PipeMonitorStage(GraphManager gm, 
						    Pipe<?>[] observedRingBuffer, 
						    Pipe<PipeMonitorSchema>[] notifyRingBuffer) {
		
		//the observed ring buffer is NOT an input
		super(gm, NONE, notifyRingBuffer); 
		
		assert(observedRingBuffer.length == notifyRingBuffer.length);
		this.observedPipe = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		
	    
		//build this lookup to find which pipe is observed from the notification pipe lookup.
		mapPipeIdToObservedPipe = new Pipe[Pipe.totalPipes()];
		int i = notifyRingBuffer.length;
		while (--i>=0) {
			mapPipeIdToObservedPipe[ notifyRingBuffer[i].id ] = observedPipe[i];
		}
				
		//this should not be so large...		
		//logger.info("\ntelemetry is watching {} pipes",notifyRingBuffer.length);		
		
		
		this.gm = gm;
		this.setNotaFlag(PronghornStage.FLAG_MONITOR);
		
	}
	
	@Override
	public void startup() {
		int i = notifyRingBuffer.length;
		while (--i>=0) {
			Pipe.setPublishBatchSize(notifyRingBuffer[i], 0);//can not be done earlier 	  
		}
	}
	
	@Override
	public void run() {
		int i = notifyRingBuffer.length;
		while (--i>=0) {
			monitorSinglePipe(notifyRingBuffer[i], observedPipe[i]);
		}
	}

	private final static int SAMP_SIZE = Pipe.sizeOf(PipeMonitorSchema.instance, MSG_RINGSTATSAMPLE_100);
	
	private long lastHead = -2;
	private long lastTail = -1;
	
	private void monitorSinglePipe(Pipe<PipeMonitorSchema> output, Pipe<?> localObserved) {

		if ((!GraphManager.monitorAll) && GraphManager.getRingProducer(gm, localObserved.id).isMonitor() ) {
			//do not read if we are monitoring the monitor and this feature is off.			
			return;
		}		
		
		//if we can't write then do it again on the next cycle, and skip this data point.
		
		if (Pipe.hasRoomForWrite(output,SAMP_SIZE)) {
											
			long headPosition = Pipe.headPosition(localObserved);
			long tailPosition = Pipe.tailPosition(localObserved);
			
			//only write if we have new data
			if (headPosition!=lastHead || tailPosition!=lastTail) {
				
			
				final int size = Pipe.addMsgIdx(output, MSG_RINGSTATSAMPLE_100);
		
				Pipe.addLongValue(System.currentTimeMillis(), output);
				Pipe.addLongValue(lastHead = headPosition, output);
				Pipe.addLongValue(lastTail = tailPosition, output);
				Pipe.addIntValue(localObserved.lastMsgIdx, output);
				Pipe.addIntValue(localObserved.sizeOfSlabRing, output);
				Pipe.addLongValue(Pipe.totalWrittenFragments(localObserved), output);
	
				Pipe.confirmLowLevelWrite(output, size);
				Pipe.publishWrites(output);
			
			}
			
						
		} else {
			
			//if unable to write then the values are dropped.
			if (Long.numberOfLeadingZeros(dropped)!=Long.numberOfLeadingZeros(++dropped)) {			
				PronghornStage consumer = GraphManager.getRingConsumer(this.gm, output.id);
				
				logger.info("Telemetry is not consuming collected data fast enough dropped:{} rate:{}ns  {}\n consumer:{}",
						    dropped,
						    (Number)GraphManager.getNota(gm, this, GraphManager.SCHEDULE_RATE, -1),
						    output,
						    consumer);
				
			}
			//if this is happening we probably have a blocking stage which does not release the thread??
			
			
		}
	}

	public Pipe<?> getObservedPipeForOutputId(int id) {		
		return mapPipeIdToObservedPipe[id];
	}

}
