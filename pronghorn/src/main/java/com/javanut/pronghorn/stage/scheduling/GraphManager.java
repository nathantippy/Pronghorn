package com.javanut.pronghorn.stage.scheduling;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.network.NetGraphBuilder;
import com.javanut.pronghorn.network.TLSCertificates;
import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeConfig;
import com.javanut.pronghorn.pipe.PipeMonitor;
import com.javanut.pronghorn.pipe.PipePublishListener;
import com.javanut.pronghorn.pipe.util.ISOTimeFormatterLowGC;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.javanut.pronghorn.stage.monitor.PipeMonitorSchema;
import com.javanut.pronghorn.stage.monitor.PipeMonitorStage;
import com.javanut.pronghorn.stage.route.ReplicatorStage;
import com.javanut.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.javanut.pronghorn.stage.test.PipeCleanerStage;
import com.javanut.pronghorn.struct.StructRegistry;
import com.javanut.pronghorn.util.AppendableBuilder;
import com.javanut.pronghorn.util.AppendableByteWriter;
import com.javanut.pronghorn.util.Appendables;
import com.javanut.pronghorn.util.ma.RunningStdDev;

public class GraphManager {
	
	private static final byte[] HR = "hr".getBytes();
	private static final byte[] MIN = "min".getBytes();
	private static final byte[] SEC = "sec".getBytes();
	private static final byte[] MS = "ms".getBytes();
	private static final byte[] MICROS = "µs".getBytes(Charset.forName("UTF-8"));
	private static final byte[] NS = "ns".getBytes();
	
	private static final byte[] AQUOTE = "\"".getBytes();
	private static final byte[] ELAP = " Elap:".getBytes();
	private static final byte[] CLOSEBRACKET_NEWLINE = "]\n".getBytes();
	private static final byte[] LABEL_OPEN = "\"[label=\"".getBytes();
	private static final byte[] LABEL_TPS = "tps".getBytes();

	private static final byte[] WHITE_SPACE_NL = " \n".getBytes();
	private static final byte[] WHITE_SPACE = " ".getBytes();
	
	private static final String CHECK_GRAPH_CONSTRUCTION = "Check graph construction";
	
	private final static int INIT_PIPES = 128;
	final static int INIT_STAGES = 64;	
	
	//will show telemetry its self
	public static boolean monitorAll = false;
	public static boolean showScheduledRateOnTelemetry = false;
	public static boolean showThreadIdOnTelemetry = false;
	public static boolean showMessageCountRangeOnTelemetry = false;
	public static boolean showPipeIdOnTelemetry = false;
	
	public static boolean experimentalStdDev = false;
			
	//set to false when we see telemetry missing edges. 
	//TODO; still debugging this not working when there are unrelated groups. switched on.
	//TODO: this is broken for SequentialReplayer/SequentialFielWrite for MQTT
	//      we need more than one group sometimes when we have multiple arrays of similar objects..
	public static boolean combineCommonEdges = true; 
	
	//turn off to minimize memory and remove from profiler.
	public static boolean recordElapsedTime = false;//this is turned on by telemetry
	
	//used for elapsed time
	private static double percentile = .80;
	
	private MemoryLeakDetector mld = null;

	private final static Logger logger = LoggerFactory.getLogger(GraphManager.class);

	//must be set before graph starts and impacts the latency of the graph.dot calls
	public final static int TELEMTRY_SERVER_RATE = 100_000_000;
	
	
	public static void setPercentile(double value) {
		if (value>1) {
			throw new UnsupportedOperationException("Must be value between 0 and 1");
		}
		percentile = value;
	}
	
	private Number defaultScheduleRate = null;
	
    //Nota bene attachments
	public final static String SCHEDULE_RATE = "SCHEDULE_RATE"; //in ns - this is the delay between calls regardless of how long call takes
	                                                        //If dependable/regular clock is required run should not return and do it internally.
	
	
	public final static String STAGE_NAME      = "STAGE_NAME";	
	public final static String SLA_LATENCY     = "SLA_LATENCY";	
	
	public final static String PRODUCER        = "PRODUCER";//explicit so it can be found even if it has feedback inputs.
	public final static String LOAD_BALANCER   = "LOAD_BALANCER"; //this stage evenly splits traffic across outputs
	public final static String LOAD_MERGE      = "LOAD_MERGE"; //this stage consumes equal priority traffic from inputs.
	public final static String HEAVY_COMPUTE   = "HEAVY_COMPUTE"; //this stage does a lot of compute, we will avoid putting these on the same thread.
	public final static String TRIGGER         = "TRIGGER"; //this stage limits rate or flow and triggers other stages.
	public final static String ROUTER_HUB      = "ROUTER_HUB"; //this stage can become a bottleneck for traffic
	public final static String ISOLATE         = "ISOLATE"; //this stage should be isolated from its neighbors it will be put on its own thread
	public final static String UNSCHEDULED     = "UNSCHEDULED";//new nota for stages that should never get a thread (experimental)
	
	public final static String DOT_RANK_NAME   = "DOT_RANK_NAME";	
	public final static String DOT_BACKGROUND  = "DOT_BACKGROUND";
	
	public final static String THREAD_GROUP  = "THREAD_GROUP";   //new nota for stages that do not give threads back (experimental)
					
	//Used for assigning stageId and for keeping count of all known stages
	private final AtomicInteger stageCounter = new AtomicInteger();
	
	//for lookup of source id and target id from the ring id
	private int[] ringIdToStages  = new int[INIT_PIPES*2]; //stores the sourceId and targetId for every ring id.
	
	//for lookup of input ring start pos from the stage id 
	private int[] stageIdToInputsBeginIdx  = new int[INIT_STAGES];
	private int[] multInputIds = new int[INIT_PIPES]; //a -1 marks the end of a run of values
	private int topInput = 0;
	
	//for lookup of output ring start pos from the stage id
	private int[] stageIdToOutputsBeginIdx = new int[INIT_STAGES];
	private int[] multOutputIds = new int[INIT_PIPES]; //a -1 marks the end of a run of values
	private int topOutput = 0;
	
	private int[] stageIdInputsCount = new int[INIT_STAGES];
	private int[] stageIdOutputsCount = new int[INIT_STAGES];
	
	
	//for lookup of RingBuffer from RingBuffer id
	private Pipe[] pipeIdToPipe = new Pipe[INIT_PIPES];
	
	private static final Pipe[] EMPTY_PIPE_ARRAY = new Pipe[0];
	
	//for lookup of Stage from Stage id
	private PronghornStage[]  stageIdToStage = new PronghornStage[INIT_STAGES];
	private long[] stageStartTimeNs = new long[INIT_STAGES];
	private long[] stageShutdownTimeNs = new long[INIT_STAGES];
	private long[] stageRunNS = new long[INIT_STAGES];
	private int[]  stageWrkPct = new int[INIT_STAGES];
	private long[] stageLastTimeNs = new long[INIT_STAGES];
	
	//keeps histogram of Elapsed time for every stage by stageId
	//By reviewing this periodically blocking and/or long running stages can be detected
	//The "bad players" are reported back to the asking scheduler so re-scheduling can be done.
	private ElapsedTimeRecorder[] stageElapsed = new ElapsedTimeRecorder[0];

	public static void visitLongRunningStages(GraphManager gm, StageVisitor visitor) {
		//bad players are detected by looking at 
		//the std dev of elapsed time for a given percentile.
		//since we want common behavior and no anomalies we use 80%
		float percentile = .80f;
		int significantSampleCount = 100;//only after this many runs do we flag.
		double stdDevLimit = 2d;// 2 std deviations
		
		
		RunningStdDev stdDev = new RunningStdDev();
		
		boolean debug = false;
		
		if (debug) {
			logger.info("scanning for longest running stages");
		}
		
		
		int i = gm.stageElapsed.length;
		while (--i>=0) {	
			if (null!=getStage(gm,i)) {
				if (GraphManager.monitorAll || (     !getStage(gm, i).isMonitor()   ) ) {			
					long elapsedAtPercentile = ElapsedTimeRecorder.elapsedAtPercentile(gm.stageElapsed[i], percentile);
					if (debug) {
						logger.info("Stage {} Elap {} ns ",getStage(gm,i).stageId,
								Appendables.appendNearestTimeUnit(new StringBuilder(), elapsedAtPercentile).toString()
								);
					}
					if (ElapsedTimeRecorder.totalCount(gm.stageElapsed[i])>significantSampleCount) {
						RunningStdDev.sample(stdDev, elapsedAtPercentile);
						
					}
				}
			}
		}
		
		if (RunningStdDev.sampleCount(stdDev)<3) {
			return; //do nothing
		}
		
		double dev = RunningStdDev.stdDeviation(stdDev);
		double mean = RunningStdDev.mean(stdDev);		
		double limit = mean+(stdDevLimit*dev);

		i = gm.stageElapsed.length;
		while (--i>=0) {	
			
			
			
			if (GraphManager.monitorAll || (     !getStage(gm, i).isMonitor()     )) {			
				if (ElapsedTimeRecorder.totalCount(gm.stageElapsed[i])>significantSampleCount) {
					if (ElapsedTimeRecorder.elapsedAtPercentile(gm.stageElapsed[i], percentile)>limit) {
						visitor.visit(getStage(gm,i));
					}
				}
			}
		}
	}
	
	public static long elapsedAtPercentile(GraphManager gm, int stageId, float pct) {
		return ElapsedTimeRecorder.elapsedAtPercentile(gm.stageElapsed[stageId], pct);
	}
	
	private Map<Object, StringBuilder> cachedRanks;
    
	private byte[][] stageDOTNames;
    private byte[][] pipeDOTSchemaNames;
    private byte[][] pipeDOTNames;
    private byte[][] stageDOTRate;
    private String[] stageIds;
    private String[] pipeDOTConst;
    
    private static byte[][] cpuValues;
	private static byte[][] pipeFullValues;

	static {
		////////////////////////////
		//build all the Work % usage labels once
		//////////////////////////// 
		int j = 100_001;
		cpuValues = new byte[j][];
		StringBuilder builder = new StringBuilder();
		while (--j>=0) {
			builder.setLength(0);
			Appendables.appendValue(builder," Wrk:",j/1000,".");
			Appendables.appendFixedDecimalDigits(builder,j%1000,100).append("%");
			cpuValues[j]= builder.toString().getBytes();
			//System.err.println(j+" is now "+builder.toString());
		}
		
		int k = 101;
		pipeFullValues = new byte[k][];
		while (--k>=0) {
			builder.setLength(0);
			Appendables.appendFixedDecimalDigits(builder.append("Full:"), k, k<=99?10:100).append("%");
			pipeFullValues[k] = builder.toString().getBytes();
		}
		
	}
  
	////////////////////////
	
	
	
	///////////////////////
	
  
	//These objects should be shared with all clones
	private final GraphManagerStageStateData stageStateData;
	public final StructRegistry recordTypeData;
	public final long startupTimeNS;
	
	//add the nota to this list first so we have an Id associated with it
	private Object[] notaIdToKey = new Object[INIT_STAGES];
	private Object[] notaIdToValue = new Object[INIT_STAGES];
	private int[] notaIdToStageId = new int[INIT_STAGES];
	private int totalNotaCount = 0;

	//store the notas ids here
	private int[] stageIdToNotasBeginIdx = new int[INIT_STAGES];
	private int[] multNotaIds = new int[INIT_PIPES]; //a -1 marks the end of a run of values
	private int topNota = 0;
	
	//this is never used as a runtime but only as a construction lock
	private Object lock = new Object();	
	
	private boolean enableMutation = true;
	public final String name;
	private transient PipeMonitorCollectorStage telemetryMonitor; //only used when we need to log telemetry and not run HTTP server.
	private transient ISOTimeFormatterLowGC telemetryTimestamp;
	private transient AppendableBuilder telemetryBuffer;
	
	public GraphManager() {
		this(null);
	}
	
	public GraphManager(String name) {
		Arrays.fill(ringIdToStages, -1);
		Arrays.fill(stageIdToInputsBeginIdx, -1);
		Arrays.fill(multInputIds, -1);
		Arrays.fill(stageIdToOutputsBeginIdx, -1);
		
		Arrays.fill(stageIdInputsCount, -1);
		Arrays.fill(stageIdOutputsCount, -1);
				
		Arrays.fill(multOutputIds, -1);
		Arrays.fill(notaIdToStageId, -1);
		Arrays.fill(stageIdToNotasBeginIdx, -1);
		Arrays.fill(multNotaIds, -1);
		this.startupTimeNS = System.nanoTime();
		this.stageStateData = new GraphManagerStageStateData();
		this.recordTypeData = new StructRegistry();
		this.name = name;		
	}
	
	private GraphManager(GraphManagerStageStateData parentStageStateData, StructRegistry parentRecordTypeData, String name) {
		Arrays.fill(ringIdToStages, -1);
		Arrays.fill(stageIdToInputsBeginIdx, -1);
		Arrays.fill(multInputIds, -1);
		Arrays.fill(stageIdToOutputsBeginIdx, -1);
		
		Arrays.fill(stageIdInputsCount, -1);
		Arrays.fill(stageIdOutputsCount, -1);
		
		Arrays.fill(multOutputIds, -1);
		Arrays.fill(notaIdToStageId, -1);
		Arrays.fill(stageIdToNotasBeginIdx, -1);
		Arrays.fill(multNotaIds, -1);
		this.startupTimeNS = System.nanoTime();
		//enables single point of truth for the stages states, all clones  share this object
		this.stageStateData = parentStageStateData;
		this.recordTypeData = parentRecordTypeData;
		this.name = name;
	}
	
	

	public static GraphManager cloneAll(GraphManager m) {
		GraphManager clone = new GraphManager(m.stageStateData, m.recordTypeData, m.name);
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				copyStage(m, clone, stage);
				copyNotasForStage(m, clone, stage);	
			}
		}
		clone.stageCounter.set(m.stageCounter.get());
		return clone;
	}
	
	public static GraphManager cloneStagesWithNotaKey(GraphManager m, Object key) {
		GraphManager clone = new GraphManager(
				new GraphManagerStageStateData(),
				m.recordTypeData, m.name);
		
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				//copy this stage if it has the required key
				if (m != getNota(m, stage, key, m)) {
					copyStage(m, clone, stage);
					copyNotasForStage(m, clone, stage);
				}
			}
		}
		return clone;
	}
	
	public static int newStageId(GraphManager gm) {
	    return gm.stageCounter.incrementAndGet();//no stage id can be 0 or smaller
	}
	
   @Deprecated	
   static PronghornStage[] getStages(GraphManager m) {
        return allStages(m);
    }
   
   @Deprecated    
   static int[] getInputRingIdsForStage(GraphManager gm, int stageId) {

       int[] ringIds = new int[INIT_PIPES];
       int index = gm.stageIdToInputsBeginIdx[stageId];

       while(-1 != gm.multInputIds[index]) {
           ringIds[index] = gm.multInputIds[index++];
       }

       for(; index < INIT_PIPES; ++index) {
           ringIds[index] = -1;
       }

       return ringIds;
   }
   
   public static int countStages(GraphManager gm) {       
       return gm.stageCounter.get();
   }
	
   public static int countStagesWithNotaKey(GraphManager m, Object key) {
        
        int count = 0;
        int i = m.stageIdToStage.length;
        while (--i>=0) {
            PronghornStage stage = m.stageIdToStage[i];
            if (null!=stage) {
                //count this stage if it has the required key
                if (null != getNota(m, stage, key, null)) {
                    count++;
                }
            }
        }
        return count;
    }
	
    public static PronghornStage getStageWithNotaKey(GraphManager m, Object key, int ordinal) {
       
       int i = m.stageIdToStage.length;
       while (--i>=0) {
           PronghornStage stage = m.stageIdToStage[i];
           if (null!=stage) {
               //count this stage if it has the required key
               if (null != getNota(m, stage, key, null)) {
                   if (--ordinal<=0) {
                       return stage;
                   }
               }
           }
       }
       throw new UnsupportedOperationException("Invalid configuration. Unable to find requested ordinal "+ordinal);
    }
	
	public static GraphManager cloneStagesWithNotaKeyValue(GraphManager m, Object key, Object value) {
		GraphManager clone = new GraphManager(
				new GraphManagerStageStateData(),
				m.recordTypeData, m.name);
		
		
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				//copy this stage if it has the required key
				if (value.equals(getNota(m, stage, key, null))) {
					copyStage(m, clone, stage);
					copyNotasForStage(m, clone, stage);
				}
			}
		}
		return clone;
	}
	
	
	public static boolean validShutdown(GraphManager m) {
				
		boolean result = true;

		
		PronghornStage[] neverStarted = GraphManager.allStagesByState(m, GraphManagerStageStateData.STAGE_NEW);
		int j = neverStarted.length;
		if  (j>0) {
			result = false;
			while (--j>=0) {
				StageScheduler.logger.error("{} never completed startup",neverStarted[j]);
			}
		}		
		
		PronghornStage[] neverShutdown = GraphManager.allStagesByState(m, GraphManagerStageStateData.STAGE_STARTED);
		j = neverShutdown.length;
		if  (j>0) {
			result = false;
			while (--j>=0) {
				StageScheduler.logger.error("{} never started shutdown",neverShutdown[j]);
			}
		}
		
		PronghornStage[] neverTerminated = GraphManager.allStagesByState(m, GraphManagerStageStateData.STAGE_STOPPING);
		j = neverTerminated.length;
		if  (j>0) {
			result = false;
			while (--j>=0) {
				StageScheduler.logger.error("{} never finished shutdown",neverTerminated[j]);
			}
		}		
		
		int i = -1;
		while (++i<m.stageIdToStage.length) {
			if (null!=m.stageIdToStage[i]) {				
				if (!isStageTerminated(m, i) ) { 	
				    PronghornStage stage = getStage(m,i);
				    //if all the inputs are empty this is not where the stall will be found
				    if (!isInputsEmpty(m,stage)) {
				    
    					StageScheduler.logger.error("-------------------");//divide the log for better clarity
    					logInputs(StageScheduler.logger, m, stage);
    					StageScheduler.logger.error("  Expected stage {} to be stopped but it appears to be running. terminated:{}", stage, isStageTerminated(m, i));
    					logOutputs(StageScheduler.logger, m, stage);
    					StageScheduler.logger.error("-------------------");//divide the log for better clarity
    					
    					result = false;
    					
    					reportUnexpectedThreadStacks(m);
					
			        }
				}				
			}
		}		
		if (!result) {
			StageScheduler.logger.error("unclean shutdown");				
		}
		return result;
	}

    private static void reportUnexpectedThreadStacks(GraphManager m) {
        Map<Thread, StackTraceElement[]> allTraces = Thread.getAllStackTraces();
        for(Entry<Thread, StackTraceElement[]> item: allTraces.entrySet()) {
            
            StackTraceElement[] ste = item.getValue();
            boolean ignore = false;
            int j = ste.length;
            while (--j>=0) {
                //These are common to all of Java and normal ways to block a thread
                ignore |= ste[j].toString().contains("getAllStackTraces");					        
                ignore |= ste[j].toString().contains("java.lang.Object.wait");
                ignore |= ste[j].toString().contains("java.util.concurrent.locks.LockSupport.parkNanos");	
                ignore |= ste[j].toString().contains("junit.runner");
                
                //This is common from hazelcast
                ignore |= ste[j].toString().contains("com.hazelcast");  
               
                
            }
            
            j = ste.length;
            if (j>0) {
                //These are very frequent at the bottom of Pronghorn stack.
        	    ignore |= ste[0].toString().contains("ThreadPerStageScheduler.continueRunning");
        	    ignore |= ste[0].toString().contains("com.javanut.pronghorn.stage.scheduling.GraphManager");
        	    if (!ignore) {
        	            			
        	        String threadName = item.getKey().getName();
        	        
        	        int idIdx = threadName.indexOf("id:");
        	        if (idIdx>=0) {
        	            int stageId = Integer.valueOf(threadName.substring(idIdx+3));
        	            if (isInputsEmpty(m, stageId)) {
        	                continue;//do next, this one has nothing blocking.
        	            }
        	        }
        	    
        	    
        		    System.err.println("");
        		    System.err.println(item.getKey().getName());
        		    while (--j>=0) {
        		        System.err.println("   "+ste[j]);
        		    }
        	    }
            }					    
        }
    }

	private static void copyNotasForStage(GraphManager m,	GraphManager clone, PronghornStage stage) {
		int idx;
		int notaId;
		int stageId = stage.stageId;
		
		idx = m.stageIdToNotasBeginIdx[stageId];
		while (-1 != (notaId=m.multNotaIds[idx++])) {
			Object key = m.notaIdToKey[notaId];
			Object value = m.notaIdToValue[notaId];					
			addNota(clone, key, value, stage);									
		}
	}

	private static void copyStage(GraphManager m, GraphManager clone, PronghornStage stage) {
		int stageId = beginStageRegister(clone, stage);
		
		int idx;
		int ringId;
		
		idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			assert(0==Pipe.contentRemaining(m.pipeIdToPipe[ringId]));
			regInput(clone, m.pipeIdToPipe[ringId], stageId);					
		}				
		
		idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {					
			assert(0==Pipe.contentRemaining(m.pipeIdToPipe[ringId]));
			regOutput(clone, m.pipeIdToPipe[ringId], stageId);					
		}		
		
		endStageRegister(clone, stage);
	}
	
    /**
     * This graph is now complete and we are ready to add monitoring and begin thread scheduling.
     * @param gm
     */
	public static void disableMutation(GraphManager gm) {
		
		if (gm.enableMutation) {
					
			logger.trace("disable mutation");

			if (gm.telemetryPort > 0) {
				recordElapsedTime = true; //turn on for the chart data
				logger.trace("enable telemetry");
				//NB: this is done very last to ensure all the pipes get monitors added.
				NetGraphBuilder.telemetryServerSetup(gm.telemetryCert, 
						gm.telemetryHost, gm.telemetryPort, 
						gm, TELEMTRY_SERVER_RATE);
				logger.trace("total count of stages including telemetry {} ",gm.stageCounter.get());
			} else {
				if (0 == gm.telemetryPort) {
					recordElapsedTime = true; //Required to see CPU allocation.
					
					//we have no web server but but poll to the log periodically.
					//this happens much less often so we lower the rate
					gm.telemetryMonitor = PipeMonitorCollectorStage.attach(gm, 
							//60 seconds       
							60*10 * Long.valueOf(GraphManager.TELEMTRY_SERVER_RATE) );
					gm.telemetryTimestamp = new ISOTimeFormatterLowGC(true);
					gm.telemetryBuffer = new AppendableBuilder();		
					
				} else {
					logger.trace("normal startup without telemetry");
				}
			}
			
			gm.enableMutation = false;
			final int totalStages = GraphManager.countStages(gm)+1;
			gm.stageDOTNames = new byte[totalStages][];
			gm.stageDOTRate = new byte[totalStages][]; 
			gm.stageIds = new String[totalStages];
			gm.pipeDOTSchemaNames = new byte[Pipe.totalPipes()][];
			gm.pipeDOTConst = new String[Pipe.totalPipes()];
			gm.pipeDOTNames = new byte[Pipe.totalPipes()][];
			
			////////////////////////
			//build all the constant string values for DOT.
			///////////////////////
			
		    int i = -1;
	        while (++i<gm.stageIdToStage.length) {
	            PronghornStage stage = gm.stageIdToStage[i];
	            if (null!=stage) {
		        	byte[] stageDisplayName = gm.stageDOTNames[stage.stageId];
		        	if (null==stageDisplayName) {
		        		buldStageDOTName(gm, stage);
		        	}
		        	
		        	if (showScheduledRateOnTelemetry) {
			        	byte[] stageRate = gm.stageDOTRate[stage.stageId];
		            	if (null==stageRate) {
		            		stageRate = buildStageDOTRate(gm, stage);
		            	}
		        	}
	            	
	            	String stageId = gm.stageIds[stage.stageId];
	            	if (null == stageId) {
	            		stageId = "Stage"+stage.stageId;
	            		gm.stageIds[stage.stageId] = stageId;
	            	}
	            }
	        }
	        
	        int j = gm.pipeIdToPipe.length;
	        while (--j>=0) {
	            Pipe pipe = gm.pipeIdToPipe[j];
	            if (null!=pipe) {
	                byte[] pipeName = gm.pipeDOTSchemaNames[pipe.id];
	                if (null==pipeName) {//keep so this is not built again upon every call
	                	
	                	String temp = Pipe.schemaName(pipe).replace("Schema", "");
	                	gm.pipeDOTSchemaNames[pipe.id] = temp.getBytes();
	                }
	            }
	        }
	          
			
        	
		}
	}

	/**
	 * If feature was enabled by setting telemetry port to zero this method uses logging to write out the dot data.
	 * @param gm
	 */
	public static void logTelemetrySnapshot(GraphManager gm) {
		if (null!=gm.telemetryBuffer) {
			gm.telemetryBuffer.clear();
			gm.telemetryTimestamp.write(System.currentTimeMillis(), gm.telemetryBuffer);
			gm.telemetryBuffer.append(".dot\n\n");
			gm.telemetryMonitor.writeAsDot(gm, "gl", gm.telemetryBuffer);
			logger.info("\nTelemetry:{}\n\n", gm.telemetryBuffer
					.toString().replaceAll("µs", "micro")); //UTF8 support offten missing from log subsystem
		} else {
			if (gm.telemetryPort<0) {
				logger.warn("requested telemetry snapshot but this feature is not enabled, turn on telemetry with port set to zero");
			}
		}
	}

	public static boolean isTelemetryEnabled(GraphManager gm) {
		return gm.telemetryPort >= 0;
	}
	
	private Comparator<? super Pipe> joinFirstComparator;
	
	public static Comparator<? super Pipe> joinFirstComparator(GraphManager m) {
		assert(!m.enableMutation) : "must not call until we are done with contruction";
		if (null==m.joinFirstComparator) {
			m.joinFirstComparator = new JoinFirstComparator(m);			
		}
		return m.joinFirstComparator;
	}
	
	//Should only be called by methods that are protected by the lock
	private static int[] setValue(int[] target, int idx, int value, Object obj) {		
		int[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, -1);
		}
		//Not an assert because this is critical and done upon startup
		if (-1!=result[idx]) {
			throw new UnsupportedOperationException("duplicate assignment detected, see stack and double check all the stages added to the graph. \ncheck: "
												   + stringWrap(obj)
					                               + " index:"+idx+" value: "+value+" foundValue: "+result[idx]);	
		}
		result[idx] = value;
		return result;
	}
	
	private static int[] setValueGrow(int[] target, int idx) {		
		int[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, -1);
		}	
		return result;
	}
	
	private static String stringWrap(Object obj) {
		try {
			return obj.toString();
		} catch (Exception e) {			
		}
		return obj.getClass().getSimpleName();
	}

	private static <T> T[] setValue(T[] target, int idx, T value) {		
		T[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		result[idx] = value;
		return (T[])result;
	}	
	
	private static <T> T[] setValueGrow(T[] target, int idx) {		
		T[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		return (T[])result;
	}	
	
	private static byte[] setValue(byte[] target, int idx, final byte value) {
		byte[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}
		
		result[idx] = value;
		return result;
	}
	
	private static byte[] setValueGrow(byte[] target, int idx) {
		byte[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}
		return result;
	}
		
	private static int[] setValue(int[] target, int idx, final int value) {
		int[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}		
		result[idx] = value;
		return result;
	}
	
	private static long[] setValue(long[] target, int idx, final long value) {
		long[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}		
		result[idx] = value;
		return result;
	}
	
	private static long[] incValue(long[] target, int idx, final long value) {
		long[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}		
		result[idx] += value;
		return result;
	}
	
	public static void register(GraphManager gm, PronghornStage stage, Pipe[] inputs, Pipe[] outputs) {
		
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			//grow if needed
			int j = inputs.length;
			if (j > 2) {
				int maxId = 0;
				while (--j>=0) {
					if (null != inputs[j]) {
						maxId = Math.max(maxId, inputs[j].id);
					}
				}
				gm.ringIdToStages = setValueGrow(gm.ringIdToStages, (maxId*2)+1); //source  then target +1
				gm.pipeIdToPipe = setValueGrow(gm.pipeIdToPipe, maxId);
				gm.multInputIds = setValueGrow(gm.multInputIds, gm.topInput+inputs.length);
			}
			
			int i=0;
			int limit = inputs.length;
			gm.stageIdInputsCount[stage.stageId]=0;
			while (i<limit) {
				try {
					regInput(gm, inputs, stageId, i, inputs[i++]);
				} catch (AssertionError e) {
					logger.error("\nError in registering input idx "+(i-1)+" of "+inputs.length+" pipe: "+inputs[i-1]+" stage:"+stage);
					throw e;
				}
			}
			
			//grow if needed
			j = outputs.length;
			if (j>2) {
				int maxId = 0;
				while (--j>=0) {
					if (null != outputs[j]) {
						maxId = Math.max(maxId, outputs[j].id);
					}
				}
				gm.ringIdToStages = setValueGrow(gm.ringIdToStages, (maxId*2)); //source  then target +1
				gm.pipeIdToPipe = setValueGrow(gm.pipeIdToPipe, maxId);				
				gm.multOutputIds = setValueGrow(gm.multOutputIds, gm.topOutput+outputs.length);
			}
			
			
			//loop over outputs
			i = 0;
			limit = outputs.length;
			gm.stageIdOutputsCount[stage.stageId]=0;
			while (i<limit) {
				try {
					regOutput(gm, outputs, stageId, i, outputs[i++]);
				} catch (AssertionError e) {
					logger.error("\nError in registering output idx "+(i-1)+" of "+outputs.length+" pipe: "+outputs[i-1]+" stage:"+stage);
					throw e;
				}
			}
			
			endStageRegister(gm, stage);
									
		}
		
	}
	
	//Experimental idea, not yet in use,  what if stages did not need to pass in pipes but could grab the ones needed by type matching??
	   private static void register(GraphManager gm, PronghornStage stage, MessageSchema inputSchema, MessageSchema outputSchema) {
	        
	        synchronized(gm.lock) {
	            int stageId = beginStageRegister(gm, stage);
	            setStateToNew(gm, stageId);
	            
	            if (null != inputSchema) {
	                int p = gm.pipeIdToPipe.length;
	                while (--p>=0) {
	                    Pipe tp = gm.pipeIdToPipe[p];
	                    if (null != tp) {
	                        if (tp.isForSchema(tp, inputSchema)) {
	                            regInput(gm, tp, stageId);
	                        }
	                    }
	                }
	            }
	            
	            if (null != outputSchema) {
                    int p = gm.pipeIdToPipe.length;
                    while (--p>=0) {
                        Pipe tp = gm.pipeIdToPipe[p];
                        if (null != tp) {
                            if (tp.isForSchema(tp, outputSchema)) {
                                regOutput(gm, tp, stageId);
                            }
                        }
                    }
                }
	            
	            endStageRegister(gm, stage);
	        }
	}
	
	/**
	 * Returns all pipes of this time in the same order that they were created in the graph.
	 * @param gm
	 * @param targetSchema
	 */
	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipesOfType(GraphManager gm, T targetSchema) {
	    return (Pipe<T>[])pipesOfType(0, gm.pipeIdToPipe.length, gm, targetSchema.getClass());
	}
	
	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipesOfTypeWithNoConsumer(GraphManager gm, T targetSchema) {
	    return (Pipe<T>[])pipesOfTypeWithNoConsumer(0, gm.pipeIdToPipe.length, gm, targetSchema.getClass());
	}
	
	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipesOfTypeWithNoProducer(GraphManager gm, T targetSchema) {
	    return (Pipe<T>[])pipesOfTypeWithNoProducer(0, gm.pipeIdToPipe.length, gm, targetSchema.getClass());
	}

	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipesOfType(GraphManager gm, T targetSchema, int minimumPipeId) {
	    return pipesOfType(0, gm.pipeIdToPipe.length, gm, targetSchema, minimumPipeId);
	}
	
	private static <T extends MessageSchema<T>> Pipe<T>[] pipesOfType(int count, int p, GraphManager gm, Class<T> targetSchemaClazz) {
		//pass one to count all the instances
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
            if ((null != tp) && Pipe.isForSchema(tp, targetSchemaClazz)) {
                	Pipe<T>[] result = pipesOfType(count+1,p,gm,targetSchemaClazz);
                	result[(result.length-1)-count] = tp;
                    return result;
            }
        }
        return (0==count) ? EMPTY_PIPE_ARRAY : new Pipe[count];
	}
	
	private static <T extends MessageSchema<T>> Pipe<T>[] pipesOfTypeWithNoConsumer(int count, int p, GraphManager gm, Class<T> targetSchemaClazz) {
		//pass one to count all the instances
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
                        
            if ((null != tp) && Pipe.isForSchema(tp, targetSchemaClazz) && (getRingConsumerId(gm, tp.id)==-1)) {
                	Pipe<T>[] result = pipesOfTypeWithNoConsumer(count+1,p,gm,targetSchemaClazz);
                	result[(result.length-1)-count] = tp;
                    return result;
            }
        }
        return (0==count) ? EMPTY_PIPE_ARRAY : new Pipe[count];
	}
	
	private static <T extends MessageSchema<T>> Pipe<T>[] pipesOfTypeWithNoProducer(int count, int p, GraphManager gm, Class<T> targetSchemaClazz) {
		//pass one to count all the instances
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
                        
            if ((null != tp) && Pipe.isForSchema(tp, targetSchemaClazz) && (getRingProducerId(gm, tp.id)==-1)) {
                	Pipe<T>[] result = pipesOfTypeWithNoProducer(count+1,p,gm,targetSchemaClazz);
                	result[(result.length-1)-count] = tp;
                    return result;
            }
        }
        return (0==count) ? EMPTY_PIPE_ARRAY : new Pipe[count];
	}
	
	
	private static <T extends MessageSchema<T>> Pipe<T>[] pipesOfType(int count, int p, GraphManager gm, T targetSchema, int minimumPipeId) {
		//pass one to count all the instances
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
                if (Pipe.isForSchema(tp, targetSchema) && tp.id>=minimumPipeId) {
                	Pipe<T>[] result = (Pipe<T>[])pipesOfType(count+1,p,gm, (Class<T>)targetSchema.getClass());
                	result[(result.length-1)-count] = tp;
                    return result;
                }
            }
        }
        
        if (0==count) {
        	return EMPTY_PIPE_ARRAY;
        } else {
        	return new Pipe[count];
        }
	    
	}
	
	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipes(GraphManager gm) {
	    
	    //pass one to count all the instances
	    int count = 0;
	    int p = gm.pipeIdToPipe.length;
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
            	count++;
            }
        }
        
        if (0==count) {
        	return EMPTY_PIPE_ARRAY;
        }
	    
        //pass two to collect all the instances.
        Pipe[] result = new Pipe[count];
        p = gm.pipeIdToPipe.length; 
        while (--p>=0) { //we are walking backwards over the pipes added
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
            	result[--count] = tp; //so we add them backwards to the input array
            }
        }
	    //the order of this array will be the same order that the pipes were added to the graph.
        return result;
	}
	   

	private static void endStageRegister(GraphManager gm, Object obj) {
		gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1, obj);
		gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1, obj);
		gm.multNotaIds = setValue(gm.multNotaIds, gm.topNota++, -1, obj);
	}

	private static int beginStageRegister(GraphManager gm, PronghornStage stage) {
		
		assert(gm.enableMutation): "Can not mutate graph, mutation has been disabled";
		
		int stageId = stage.stageId;
		
		assert(stageId>=gm.stageIdToStage.length || null==gm.stageIdToStage[stageId]) : "Can only register the same stage once";
				
		//now store the stage
		gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
		gm.stageIdToInputsBeginIdx = setValue(gm.stageIdToInputsBeginIdx, stageId, gm.topInput, stage);
		gm.stageIdToOutputsBeginIdx = setValue(gm.stageIdToOutputsBeginIdx, stageId, gm.topOutput, stage);		
		
		gm.stageIdInputsCount = setValue(gm.stageIdInputsCount, stageId, 0); 
		gm.stageIdOutputsCount = setValue(gm.stageIdOutputsCount, stageId, 0);
		
		gm.stageIdToNotasBeginIdx = setValue(gm.stageIdToNotasBeginIdx, stageId, gm.topNota, stage);		
		gm.stageRunNS = setValue(gm.stageRunNS, stageId, 0);
		gm.stageShutdownTimeNs = setValue(gm.stageShutdownTimeNs, stageId, 0);
		gm.stageStartTimeNs = setValue(gm.stageStartTimeNs, stageId, 0);
		
		gm.stageLastTimeNs = setValue(gm.stageLastTimeNs, stageId, 0);
		gm.stageWrkPct = setValue(gm.stageWrkPct, stageId, 0);
		
		
		//add defaults if a value is not already present
		int d = gm.defaultsCount;
		while(--d >= 0) {
		    //stage does not have the default value so set it
		    GraphManager.addNota(gm, gm.defaultNotaKeys[d], gm.defaultNotaValues[d], stage);
		}
		
		return stageId;
	}



	public static void register(GraphManager gm, PronghornStage stage, Pipe input, Pipe[] outputs) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			if (null!=input) {
				gm.stageIdInputsCount[stage.stageId]=1;
			} else {
				gm.stageIdInputsCount[stage.stageId]=0;
			}
			
			//loop over inputs
			regInput(gm, input, stageId);
			
			//grow if needed
			int j = outputs.length;
			if (j>2) {
				int maxId = 0;
				while (--j>=0) {
					if (null != outputs[j]) {
						maxId = Math.max(maxId, outputs[j].id);
					}
				}
				gm.ringIdToStages = setValueGrow(gm.ringIdToStages, (maxId*2)); //source  then target +1
				gm.pipeIdToPipe = setValueGrow(gm.pipeIdToPipe, maxId);				
				gm.multOutputIds = setValueGrow(gm.multOutputIds, gm.topOutput+outputs.length);
			}
			
			
			int i = 0;
			final int limit = outputs.length;
			gm.stageIdOutputsCount[stage.stageId]=0;
			while (i<limit) {
				regOutput(gm, outputs, stageId, i, outputs[i++]);
			}
			
			endStageRegister(gm, stage);
		}
	}

	public static void regOutput(GraphManager gm, Pipe[] outputs, int stageId, int x, Pipe localOutput) {
		if(null!=localOutput) {
					
			//if this same pipe is already in the same output array this may be done for
			//indexing of data to pipes but we only need to register the usage once
			assert(outputs[x].id == localOutput.id);
			boolean alreadyReg = false;

			while (--x>=0) {
				if (outputs[x]!=null) {
					Pipe.structRegistry(outputs[x],gm.recordTypeData);
					alreadyReg |= (localOutput.id == outputs[x].id);
				}
			}
			if (!alreadyReg) {
				gm.stageIdOutputsCount[stageId]++;
				regOutput(gm, localOutput, stageId);
			}
		}
	}
	
	public static void regInput(GraphManager gm, Pipe[] inputs, int stageId, int x, Pipe localInput) {
		if(null!=localInput) {
			
			//eliminate duplicates
			assert(inputs[x].id == localInput.id);
			boolean alreadyReg = false;

			while (--x>=0) {
				if (inputs[x]!=null) {	
					Pipe.structRegistry(inputs[x],gm.recordTypeData);
					alreadyReg |= (localInput.id == inputs[x].id);
				}
			}
			if (!alreadyReg) {
				gm.stageIdInputsCount[stageId]++;
				regInput(gm, localInput, stageId);
			}
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, Pipe[] inputs, Pipe output) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			//grow if needed
			int j = inputs.length;
			if (j > 2) {
				int maxId = 0;
				while (--j>=0) {
					if (null != inputs[j]) {
						maxId = Math.max(maxId, inputs[j].id);
					}
				}
				gm.ringIdToStages = setValueGrow(gm.ringIdToStages, (maxId*2)+1); //source  then target +1
				gm.pipeIdToPipe = setValueGrow(gm.pipeIdToPipe, maxId);
				gm.multInputIds = setValueGrow(gm.multInputIds, gm.topInput+inputs.length);
			}
			
			int i = 0;
			final int limit = inputs.length;
			gm.stageIdInputsCount[stage.stageId] = 0;
			while (i<limit) {
				regInput(gm,inputs,stageId,i,inputs[i++]);
			}
			
			if (null!=output) {
				gm.stageIdOutputsCount[stage.stageId]=1;
		    } else {
		    	gm.stageIdOutputsCount[stage.stageId]=0;
		    }
			//loop over outputs			
			regOutput(gm, output, stageId);			
			
			endStageRegister(gm, stage);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, Pipe input, Pipe output) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			if (null!=input) {
				gm.stageIdInputsCount[stage.stageId]=1;
		    } else {
		    	gm.stageIdInputsCount[stage.stageId]=0;
		    }
			//loop over inputs
			regInput(gm, input, stageId);
	
			if (null!=output) {
				gm.stageIdOutputsCount[stage.stageId]=1;
		    } else {
		    	gm.stageIdOutputsCount[stage.stageId]=0;		    	
		    }
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			endStageRegister(gm, stage);
		}
	}
	
	private static void setStateToNew(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(stageStateArray(gm), stageId, GraphManagerStageStateData.STAGE_NEW);
		}
	}
	
	public static void setStateToStopping(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(stageStateArray(gm), stageId, GraphManagerStageStateData.STAGE_STOPPING);
		}
	}

	public static void setStateToStarted(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(stageStateArray(gm), stageId, GraphManagerStageStateData.STAGE_STARTED);
			gm.stageStartTimeNs[stageId] = System.nanoTime();
		}
	}
	
	public static void setStateToShutdown(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(stageStateArray(gm), stageId, GraphManagerStageStateData.STAGE_TERMINATED);
			//	assert(recordInputsAndOutputValuesForValidation(gm, stage.stageId));
			gm.stageShutdownTimeNs[stageId] = System.nanoTime();
		}
	}
	
//	private static boolean recordInputsAndOutputValuesForValidation(GraphManager gm, int stageId) {
//		
//		//TODO: B, this is a test to see if the positions of the ring buffer have moved after shut down, would be called when the real shutdown is completed
//		
//		int ringId;
//		int idx;
//		
//		idx = gm.stageIdToInputsBeginIdx[stageId];
//		while (-1 != (ringId=gm.multInputIds[idx++])) {	
//			
//			//record the tail positions for this stage
//			
//			//gm.ringIdToRing[ringId];	//Must verify these are the same after the shutdown is complete
//			
//		}
//		
//		
//		idx = gm.stageIdToOutputsBeginIdx[stageId];
//		while (-1 != (ringId=gm.multOutputIds[idx++])) {	
//			
//			//record the head positions for this stage
//			
//			//gm.ringIdToRing[ringId];	//Must verify these are the same after the shutdown is complete	
//			
//		}	
//		
//		return true;
//	}

	private static void regOutput(GraphManager gm, Pipe output, int stageId) {
		if (null!=output) {
			int outputId = output.id;
			Pipe.structRegistry(output,gm.recordTypeData);
			try {
				gm.ringIdToStages = setValue(gm.ringIdToStages, (outputId*2) , stageId, gm.stageIdToStage[stageId]); //source  then target +1
				gm.pipeIdToPipe = setValue(gm.pipeIdToPipe, outputId, output);				
				gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, outputId, output);
			} catch (UnsupportedOperationException uoe) {
				output.creationStack();
				logger.info("\nRegister: "+output);
				throw uoe;
			}
			assert(stageId == getRingProducerId(gm, outputId));
		}
	}
	

	private static void regInput(GraphManager gm, Pipe input, int stageId) {
		if (null!=input) {
			int inputId = input.id;
			Pipe.structRegistry(input,gm.recordTypeData);
			try {
				gm.ringIdToStages = setValue(gm.ringIdToStages, (inputId*2)+1, stageId, gm.stageIdToStage[stageId]); //source  then target +1
				gm.pipeIdToPipe = setValue(gm.pipeIdToPipe, inputId, input);
				gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, inputId, input);
			} catch (UnsupportedOperationException uoe) {
				input.creationStack();
				logger.info("\nRegister: "+input);
				throw uoe;
			}
			assert(stageId == getRingConsumerId(gm, inputId));
		}
	}
	
	private final int maxDefaults = 10;
    private final Object[] defaultNotaKeys = new Object[maxDefaults];
    private final Object[] defaultNotaValues = new Object[maxDefaults];
    private int defaultsCount = 0;    
	
	
    public Number defaultScheduleRate() {
    	return defaultScheduleRate;
    }
    
    public static void addDefaultNota(GraphManager graphManager, Object key, Object value) {
        
    	if (key==SCHEDULE_RATE) {    	
    		graphManager.defaultScheduleRate = (Number)value;
    	}
        graphManager.defaultNotaKeys[graphManager.defaultsCount] = key;
        graphManager.defaultNotaValues[graphManager.defaultsCount++] = value;
        
    }


	public static void addNota(GraphManager graphManager, Object key, Object value, PronghornStage ... stages) {
	    
		int i = stages.length;
		if (0==i) {
		    throw new UnsupportedOperationException("Must have at least 1 stage to assign this to");
		}
		while (--i>=0) {
			addNota(graphManager, key, value, stages[i]);
		}
	}
	
	public static void addNota(GraphManager m, Object key, Object value, PronghornStage stage) {
		addNota(m, key, value, stage.stageId);
	}
	
	public static void addNota(GraphManager m, Object key, Object value, int stageId) {
		synchronized(m.lock) {	
			
			//if Nota key already exists then replace previous value
			
			int notaId = findNotaIdForKey(m, stageId, key);
			
			if (-1 != notaId) {
				//simple replace with new value
				m.notaIdToValue[notaId] = value;				
			} else {
				//extract as add nota method.
				//even if the same key/value is given to multiple stages we add it multiple times here
				//this allows for direct lookup later for every instance found
				m.notaIdToKey = setValue(m.notaIdToKey, m.totalNotaCount, key);
				m.notaIdToValue = setValue(m.notaIdToValue, m.totalNotaCount, value);
				m.notaIdToStageId = setValue(m.notaIdToStageId, m.totalNotaCount, stageId);
				
				int beginIdx = m.stageIdToNotasBeginIdx[stageId];
			    if (m.topNota == m.multNotaIds.length) {
			    	
			    	//create new larger array		    	
			    	int[] newMultiNotaIdx = new int[m.multNotaIds.length*2];			    	
			    	Arrays.fill(newMultiNotaIdx, -1);
			    	
			    	System.arraycopy(m.multNotaIds, 0, newMultiNotaIdx, 0, beginIdx);
			    	
			    	//copy over and move it down by one so we have room for the new entry
			    	System.arraycopy(m.multNotaIds, beginIdx, newMultiNotaIdx, beginIdx+1, m.topNota-(beginIdx));
			    	
			    	m.multNotaIds = newMultiNotaIdx;		    	
			    } else {
			    	
			    	//move all the data down by one so we have room.
			    	System.arraycopy(m.multNotaIds, beginIdx, m.multNotaIds, beginIdx+1, m.topNota-(beginIdx));
			    }
			    
			    m.multNotaIds[beginIdx] = m.totalNotaCount;//before we inc this value is the index to this key/value pair
			    
				m.totalNotaCount++;
				m.topNota++;
				
			}
			
		}
	}
	
	//also add get regex of key string
	
	private static int findNotaIdForKey(GraphManager m, int stageId, Object key) {
		int idx = m.stageIdToNotasBeginIdx[stageId];
		if (idx>=m.multNotaIds.length) {
			return -1;
		}
		int notaId;
		while(idx<m.multNotaIds.length && (-1 != (notaId = m.multNotaIds[idx]))) {
			if (m.notaIdToKey[notaId].equals(key)) {
				return notaId;
			}
			idx++;
		}		
		return -1;
	}
	

	
	/**
	 * Returns nota if one is found by this key on this stage, if nota is not found it returns the defaultValue
	 * @param m
	 * @param stage
	 * @param key
	 */
	public static Object getNota(GraphManager m, PronghornStage stage, Object key, Object defaultValue) {
	    return (null==stage) ? defaultValue : getNota(m, stage.stageId, key, defaultValue);
	}
	
	public static boolean hasNota(GraphManager m, int stageId, Object key) {
		return (null != getNota(m, stageId, key, null));
	}
	
	public static Object getNota(GraphManager m, int stageId, Object key, Object defaultValue) {
	    if (stageId>=0) {
    		int idx = m.stageIdToNotasBeginIdx[stageId];
    		if (idx>=0 && idx<m.multNotaIds.length) {
	    		int notaId;
	    		while(-1 != (notaId = m.multNotaIds[idx])) {
	    			if (m.notaIdToKey[notaId].equals(key)) {
	    				return m.notaIdToValue[notaId];
	    			}
	    			idx++;
	    		}
    		}
	    }
		return defaultValue;
	}
	
	public static void shutdownNeighborRings(GraphManager pm, PronghornStage baseStage) {
		
		int inputPos  = pm.stageIdToInputsBeginIdx[baseStage.stageId];
	    int outputPos =	pm.stageIdToOutputsBeginIdx[baseStage.stageId];		
		
	    int ringId;
	    
	    while ((ringId = pm.multInputIds[inputPos++])>=0) {
	    	Pipe.shutdown(pm.pipeIdToPipe[ringId]);	
	    }
	    
	    while ((ringId = pm.multOutputIds[outputPos++])>=0) {
		    Pipe.shutdown(pm.pipeIdToPipe[ringId]);	
		}
	}

	public static PronghornStage getStage(GraphManager m, int stageId) {
		return m.stageIdToStage[stageId];
	}
	
	public static Pipe getPipe(GraphManager gm, int ringId) {
		return gm.pipeIdToPipe[ringId];
	}
	
	public static PronghornStage getRingProducer(GraphManager gm, int ringId) {
		int stageId = getRingProducerId(gm, ringId);
		if (stageId<0) {
		    throw new UnsupportedOperationException("Can not find input stage writing to pipe "+ringId+". Check graph construction.");
		}
        return  gm.stageIdToStage[stageId];
	}

    static int getRingProducerId(GraphManager gm, int ringId) {
        int idx = ringId<<1;
		return idx < gm.ringIdToStages.length ? gm.ringIdToStages[idx] : -1;
    }
	
   public static int getRingProducerStageId(GraphManager gm, int ringId) {
        return getRingProducerId(gm, ringId);
    }
	
	public static PronghornStage getRingConsumer(GraphManager gm, int ringId) {
		int stageId = getRingConsumerId(gm, ringId);
	    if (stageId<0) {
	            throw new UnsupportedOperationException("Can not find stage reading from pipe "+ringId+" Check graph construction.");
	    }
        return  gm.stageIdToStage[stageId];
	}

    public static int getRingConsumerStageId(GraphManager gm, int ringId) {
        return getRingConsumerId(gm, ringId);
    }
	   
    static int getRingConsumerId(GraphManager gm, int ringId) {
        int idx = (ringId*2)+1;
		return idx < gm.ringIdToStages.length ? gm.ringIdToStages[idx] : -1;
    }
	

	public static long delayRequiredNS(GraphManager m, int stageId) {
        long waitTime = computeDelayForConsumer(m.stageIdToInputsBeginIdx[stageId], m.pipeIdToPipe, m.multInputIds);
        return addDelayForProducer(m.stageIdToOutputsBeginIdx[stageId], waitTime, m.pipeIdToPipe, m.multOutputIds);
	}
	
   public static boolean isRateLimited(GraphManager m, int stageId) {
        Pipe[] pipeIdToPipe2 = m.pipeIdToPipe;
        
        return isRateLimitedConsumer(m.stageIdToInputsBeginIdx[stageId], pipeIdToPipe2, m.multInputIds) ||
               isRateLimitedProducer(m.stageIdToOutputsBeginIdx[stageId], pipeIdToPipe2, m.multOutputIds);
    }

    private static long computeDelayForConsumer(int pipeIdx, Pipe[] pipeIdToPipe2, int[] multInputIds2) {

        int pipeId;
        long waitTime = 0;
        while ((pipeId = multInputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedConsumer(pipeIdToPipe2[pipeId])) {
                long t = Pipe.computeRateLimitConsumerDelay(pipeIdToPipe2[pipeId]);
                if (t>waitTime) {
                    waitTime = t;
                }
            }
        }	
        return waitTime;
    }

    private static boolean isRateLimitedConsumer(int pipeIdx, Pipe[] pipeIdToPipe2, int[] multInputIds2) {

        int pipeId;
        while ((pipeId = multInputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedConsumer(pipeIdToPipe2[pipeId])) {
                return true;
            }
        }   
        return false;
    }
    
    private static long addDelayForProducer(int pipeIdx, long waitTimeNS, Pipe[] pipeIdToPipe2, int[] multOutputIds2) {
        int pipeId;
        while ((pipeId = multOutputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedProducer(pipeIdToPipe2[pipeId])) {

                long nsTime = Pipe.computeRateLimitProducerDelay(pipeIdToPipe2[pipeId]);
                if (nsTime > waitTimeNS) {
                    waitTimeNS = nsTime;
                }
            }
        }   
	    return waitTimeNS;
    }
    
    private static boolean isRateLimitedProducer(int pipeIdx, Pipe[] pipeIdToPipe2, int[] multOutputIds2) {
        int pipeId;
        while ((pipeId = multOutputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedProducer(pipeIdToPipe2[pipeId])) {
                return true;
            }
        }   
        return false;
    }
	
	/**
	 * Return false only when every path is checked so every ring is empty and every stage is terminated.
	 */
	public static boolean mayHaveUpstreamData(GraphManager m, int stageId) {
		
		if (isStageTerminated(m, stageId)) { //terminated 
			return false;
		}		
				
		int pipeId;
				
		//if all the output targets have terminated return false because this data will never be consumed
		//using this back pressure the shutdown of all stages can be done		
		int outputPos  = m.stageIdToOutputsBeginIdx[stageId];
		boolean noConsumers = true;
		int count = 0;
		while ((pipeId = m.multOutputIds[outputPos++])>=0) {
			count++;
			int ringConsumerId = GraphManager.getRingConsumerId(m, pipeId);
			noConsumers = noConsumers & (ringConsumerId<0 || isStageTerminated(m,ringConsumerId));						
		}				
		if (count>0 && noConsumers) {
			//ignore input because all the consumers have already shut down
			return false;
		}		
		
		
		int inputPos  = m.stageIdToInputsBeginIdx[stageId];
		int inputCounts=0;
		    
		while ((pipeId = m.multInputIds[inputPos++])>=0) {
							
				++inputCounts;
				
				//check that producer is terminated first.
				if (isProducerTerminated(m, pipeId)) {
					
					//ensure that we do not have any old data still on the ring from the consumer batching releases
    
					//splitter should never have release pending to release because it does not use the release counters	
				//	if (Pipe.hasReleasePending(m.pipeIdToPipe[pipeId])) {
					    Pipe.releaseAllBatchedReads(m.pipeIdToPipe[pipeId]);
			//			Pipe.releaseAll(m.pipeIdToPipe[pipeId]);
				///	}						
					
					//if producer is terminated check input ring, if not empty return true
			    	if (Pipe.contentRemaining( m.pipeIdToPipe[pipeId])>0) {
			    		return true;
			    	}
				} else {
					return true;
				}				
		}
		
		//true if this is a top level stage and was not terminated at top of this function
		//all other cases return false because the recursive check has already completed above.
		return 0==inputCounts && !isStageShuttingDown(m, stageId); //if input is shutting down it must not re-schedule
	}
	
	public static boolean isProducerTerminated(GraphManager m, int ringId) {
		int producerStageId = getRingProducerId(m, ringId);
        return producerStageId<0 || stageStateArray(m)[producerStageId] == GraphManagerStageStateData.STAGE_TERMINATED;
	}

    public static boolean isStageTerminated(GraphManager m, int stageId) {
    	synchronized(m.stageStateData.lock) {
    		return GraphManagerStageStateData.STAGE_TERMINATED <= stageStateArray(m)[stageId];
    	}
    }

    public static boolean isStageShuttingDown(final GraphManager m, final int stageId) {
    	return isStageShuttingDown(stageStateArray(m), stageId);
    }
    
    public static boolean isStageShuttingDown(final byte[] stateArray, final int stageId) {
    	return stateArray[stageId]>=GraphManagerStageStateData.STAGE_STOPPING; //or terminated
    }

	public static byte[] stageStateArray(final GraphManager m) {
		return m.stageStateData.stageStateArray;
	}
    
    public static boolean isStageStarted(GraphManager m, int stageId) {
        return stageStateArray(m)[stageId]>=GraphManagerStageStateData.STAGE_STARTED; //or running or shuttingdown or terminated
    }
    
    public static PronghornStage[] allStagesByState(GraphManager graphManager, int state) {
    	 //TODO: rewrite and simple recursive stack unroll to eliminate the duplication of the code here. see pipesOfType
        int count = 0;
        int s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null!=stage && stageStateArray(graphManager)[stage.stageId]==state) {
                count++;
            }
        }
        
        PronghornStage[] stages = new PronghornStage[count];
        s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null != stage && stageStateArray(graphManager)[stage.stageId]==state) {
                stages[--count] = stage;
            }
        }
        return stages;
    }
    
    public static PronghornStage[] allStages(GraphManager graphManager) {
        
        int count = 0;
        int s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null!=stage) {
                count++;
            }
        }
        
        PronghornStage[] stages = new PronghornStage[count];
        s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null != stage) {
                stages[--count] = stage;
            }
        }
        return stages;
    }
    
    
    //TODO: AA must have blocking base stage to extend for blockers.
    
	public static void terminateInputStages(GraphManager m) {
		
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			if (null!=m.stageIdToStage[i]) {				
				//an input stage is one that has no input ring buffers
				if (-1 == m.multInputIds[m.stageIdToInputsBeginIdx[m.stageIdToStage[i].stageId]]) {
					//terminate all stages without any inputs
					m.stageIdToStage[i].requestShutdown();
				} else if (null != getNota(m, m.stageIdToStage[i], PRODUCER, null)) {
					//also terminate all stages decorated as producers
					m.stageIdToStage[i].requestShutdown();
					//producers with inputs must be forced to terminate or the input queue will prevent shutdown
					setStateToShutdown(m, m.stageIdToStage[i].stageId); 					
				}
			}
		}
	}
	
	public static int getOutputStageCount(GraphManager m) {
	    int count = 0;
        int i = m.stageIdToStage.length;
        while (--i>=0) {
            if (null!=m.stageIdToStage[i]) {                
                //an input stage is one that has no input ring buffers
                if (-1 == m.multOutputIds[m.stageIdToOutputsBeginIdx[m.stageIdToStage[i].stageId]]) {
                    if (!stageForMonitorData(m, m.stageIdToStage[i])) {
                        count++;
                    }
                }
            }
        }
        return count;
	}
	
	   public static PronghornStage getOutputStage(GraphManager m, int ordinal) {
	        int count = 0;
	        int i = m.stageIdToStage.length;
	        while (--i>=0) {
	            if (null!=m.stageIdToStage[i]) {                
	                //an input stage is one that has no input ring buffers
	                if (-1 == m.multOutputIds[m.stageIdToOutputsBeginIdx[m.stageIdToStage[i].stageId]]) {
	                    if (!stageForMonitorData(m, m.stageIdToStage[i])) {
    	                    if (++count==ordinal) {
    	                        return m.stageIdToStage[i];
    	                    }
	                    }
	                }
	            }
	        }
	        throw new UnsupportedOperationException("Invalid configuration. Unable to find requested output ordinal "+ordinal);
	    }

	public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, PronghornStage stage) {
		return getOutputPipe(m, stage, 1);
	}
	
	public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, int stageId) {
		return getOutputPipe(m, stageId, 1);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, PronghornStage stage, int ordinalOutput) {
		return getOutputPipe(m, stage.stageId, ordinalOutput);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, int stageId, int ordinalOutput) {
		assert(ordinalOutput>0);
		assert(ordinalOutput<=getOutputPipeCount(m,stageId));
		
		return m.pipeIdToPipe[m.multOutputIds[m.stageIdToOutputsBeginIdx[stageId]+ordinalOutput-1]];

	}
	
	public static int getOutputPipeCount(GraphManager m, int stageId) {
		assert (checkOutputsCount(m, stageId) == ((stageId>=0)?m.stageIdOutputsCount[stageId] : 0) ) : checkOutputsCount(m, stageId)+" == "+((stageId>=0)?m.stageIdOutputsCount[stageId] : 0) ;		
		return (stageId>=0) ? m.stageIdOutputsCount[stageId] : 0;
	}

	private static int checkOutputsCount(GraphManager m, int stageId) {
		int idx = m.stageIdToOutputsBeginIdx[stageId];
		int count = 0;
		if (idx>=0) {
			while (-1 != (m.multOutputIds[idx++])) {		
				count++;
			}	
		}		
		
		return count;
	}

	public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, PronghornStage stage) {
		return getInputPipe(m, stage, 1);
	}
	
	public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, int stageId) {
		return getInputPipe(m, stageId, 1);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, PronghornStage stage, int ordinalInput) {
		return getInputPipe(m, stage.stageId, ordinalInput);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, int stageId, int ordinalInput) {
		assert(ordinalInput>0);
		assert(ordinalInput<=getInputPipeCount(m,stageId));
		return m.pipeIdToPipe[m.multInputIds[m.stageIdToInputsBeginIdx[stageId]+ordinalInput-1]];
	}
	
    public static <S extends MessageSchema<S>> int getInputPipeId(GraphManager m, int stageId, int ordinalInput) {
    	assert(ordinalInput>0);
    	assert(ordinalInput<=getInputPipeCount(m,stageId));
    	int idx = m.stageIdToInputsBeginIdx[stageId];
	   	assert(m.multInputIds[idx+ordinalInput-1]>=0) : "bad value";
    	return m.multInputIds[idx+ordinalInput-1];    	
    }
	
	public static int getInputPipeCount(GraphManager m, PronghornStage stage) {
		return getInputPipeCount(m, stage.stageId);
	}
	
	public static int getInputPipeCount(GraphManager m, int stageId) {
		assert (checkInputsCount(m,stageId) == ((stageId>=0) ? m.stageIdInputsCount[stageId] : 0)) : checkInputsCount(m,stageId)+" == "+((stageId>=0) ? m.stageIdInputsCount[stageId] : 0);
		return (stageId>=0) ? m.stageIdInputsCount[stageId] : 0;
	}

	private static int checkInputsCount(GraphManager m, int stageId) {
		int count = 0;
		if (stageId>=0) {
			int idx = m.stageIdToInputsBeginIdx[stageId];
			if (idx>=0) {
				while (-1 != (m.multInputIds[idx++])) {	
					count++;	
				}	
			}
		}
		return count;
	}
	
	public void exportGraphDotFile() {
	    exportGraphDotFile(this, "graph.dot", true);
	}
	
	public static void exportGraphDotFile(GraphManager gm, String filename, boolean isVertical) {
		exportGraphDotFile(gm,filename,isVertical,null,null,null);
	}
	
    public static void exportGraphDotFile(GraphManager gm, String filename, boolean isVertical,
    		                              int[] percentileValues, long[] traffic, int[] msgPerSec) {
    	
    //	new Exception("GENERATING NEW DOT FILE "+filename).printStackTrace();
    	
        FileOutputStream fost;
        try {            
            AppendableBuilder target = new AppendableBuilder(1<<24);
          
            gm.writeAsDOT(gm, filename, target, isVertical, percentileValues, traffic, msgPerSec);

            fost = new FileOutputStream(filename);
            target.copyTo(fost);
            fost.close();
            
            
            //to produce the png we must call
            //  dot -Tpng -O deviceGraph.dot        
            Process result = Runtime.getRuntime().exec("dot -Tsvg -o"+filename+".svg "+filename);
            
            if (0!=result.waitFor()) {
                return;
            }
            
        } catch (Throwable e) {        	
        	logger.trace("No runtime graph produced. ",e);
        }
       
        
    }
    
    public static void writeAsDOT(GraphManager m, AppendableBuilder target, boolean isVertical) {
    	
    	writeAsDOT(m,/*optional dynamic rollover text */null,target,isVertical,null,null,null);
    }
    
    
    private static final byte[] SUMMARY_MAXPIPEFILL = "{\"maxPipeFill\":".getBytes();
    private static final byte[] SUMMARY_MAXCPUPCT = ",\"maxCPUPct\":".getBytes();
    private static final byte[] SUMMARY_CLOSE = "}".getBytes();

    
    public static void writeAsSummary(GraphManager m, AppendableByteWriter<?> target,
            						 int[] pipePercentileFullValues) {

    	int maxPctFull = 0;
    	int j = pipePercentileFullValues.length;
    	while (--j>=0) {
    		maxPctFull = Math.max(maxPctFull, pipePercentileFullValues[j]);    		
    	}
    	////////
    	////////
    	int maxCPUPct = 0;    	
        int i = -1;
        while (++i<m.stageIdToStage.length) {
        	 PronghornStage stage = m.stageIdToStage[i];
	            
	         if (mustShowOnTelemetry(m, stage)) {       
	            	
	            	long runNs = m.stageRunNS[stage.stageId];
	            	if (runNs>0) {
	            		maxCPUPct = Math.max(maxCPUPct, m.stageWrkPct[stage.stageId]);//moving average of CPU
	            	}
	         }        	
        }
    	////////
    	////////
        
		target.write(SUMMARY_MAXPIPEFILL);
        Appendables.appendValue(target, maxPctFull);
		target.write(SUMMARY_MAXCPUPCT);
        Appendables.appendDecimalValue(target, maxCPUPct, (byte)-3);
		target.write(SUMMARY_CLOSE);
        
    }
    
    
	public static void writeAsDOT(GraphManager m, String graphName, 
								  AppendableByteWriter<?> target, boolean isVertical,
			                      int[] pipePercentileFullValues, 
			                      long[] pipeTraffic, int[] msgPerSec) {
			
	        int stages = GraphManager.countStages(m);
	        	        
	        
	        Map<Object, StringBuilder> ranks = m.cachedRanks; 
	        
	        target.write("digraph ".getBytes());
	        if (null!=graphName) {
	        	target.append(graphName);
	        } else {
	        	if (null!=m.name) {
	        		target.append(m.name);
	        	}
	        }
	        target.write(" {\n".getBytes()); 
	     //   target.append("concentrate=true\n");

			//add new font and black background color
			target.write("bgcolor=\"#000000\"".getBytes());
			target.write("bgcolor=\"#000000\"".getBytes());
			target.write("graph [fontname = \"arial\"];".getBytes());
			target.write("node [fontname = \"arial\", margin=0, fontsize=25];".getBytes());
			target.write("edge [fontname = \"arial\", fontsize=25, arrowsize=1.5];".getBytes());

	        if (stages<500) {
	            //no need if the picture is very large
		        if (isVertical) {
		        	target.write("rankdir = TD\n".getBytes()); 
		        } else {
		        	target.write("rankdir = LR\n".getBytes()); 
		        }
		        if (null==ranks) {
		        	ranks = new HashMap<Object, StringBuilder>();
		        }
	        }
	        
	        
	        int i = -1;
	        while (++i<m.stageIdToStage.length) {
	            PronghornStage stage = m.stageIdToStage[i];
	        	            
	            if (mustShowOnTelemetry(m, stage)) {       
          	

	            	////////////////////
	            	//cache stage names since these are needed frequently
	            	///////////////////
	            	String stageId = m.stageIds[stage.stageId];

	            	//////////////////
	            	populateRanks(m, ranks, stage, stageId);

	                target.write(AQUOTE);
	                target.append(stageId);
	                target.write(LABEL_OPEN);
	                
	                byte[] stageDisplayName = m.stageDOTNames[stage.stageId];
	                if (null!=stageDisplayName) {
	                	target.write(stageDisplayName);	        
		        	} else {
		        		stageDisplayName = buldStageDOTName(m, stage);
		        		target.write(stageDisplayName);	           	           
		        	}
	                               
	                
	                //if supported give PCT used
	                long runNs = m.stageRunNS[stage.stageId];
	                int pct = 0;
	                if (runNs!=0){
	                	if (runNs<0) {
	                		
	                		target.write(" Wrk:100%".getBytes());
	                	} else {
	                		long shutdownTime = m.stageShutdownTimeNs[stage.stageId];
	                		if (shutdownTime<=0) {
	                			//NOTE: this is required becaue FixedThreadScheduler and NonThreadScheduler quit too early before this gets set.
	                			shutdownTime = System.nanoTime();
	                		}
	                		
	                		
	                		//TODO: add params to pass in percentile and limit
	                		
	                		pct = m.stageWrkPct[stage.stageId];//moving average of CPU
	                			                		
	                		//int lifePct = (int)((100_000L*runNs)/ (shutdownTime - m.stageStartTimeNs[stage.stageId] ));             		
	                		
	                		
	                		if (pct>=0) {
	                			target.write(cpuValues[pct]);	                			
	                		} else {
	                			target.write(" Wrk:N/A%".getBytes());
	                			
	                			//logger.info("A bad % value {} {} {} {}",pct,runNs, m.stageShutdownTimeNs[stage.stageId],  m.stageStartTimeNs[stage.stageId] );
	                					
	                		}
	                	}
	                } else {
	                	target.write(" Wrk:N/A%".getBytes());
	                	
            			//logger.trace("B bad % value {} {} {} {}",pct,runNs, m.stageShutdownTimeNs[stage.stageId],  m.stageStartTimeNs[stage.stageId] );
            	            			
	                }
	                
	                if (recordElapsedTime) {
	                	try {
	                		writeElapsed(target, ElapsedTimeRecorder.elapsedAtPercentile(m.stageElapsed[stage.stageId],(float)percentile));
	                	} catch (Throwable t) {
	                		//do not produce since we have no data.
	                	}
	                }
	                

	            	byte[] stageRate = m.stageDOTRate[stage.stageId];	            	
	                if (null!=stageRate) {
	                	target.write(stageRate);	                
	                }
	                
	                target.write(AQUOTE);

					if (pct>=75000) {
                		target.write(",color=red,penwidth=5".getBytes());	    
                	} else if (pct>=40000) {
                		target.write(",color=orange,penwidth=5".getBytes());	    
                	} else if (pct>=20000) {
                		target.write(",color=blue,penwidth=5".getBytes());
                	} else {
						target.write(",color=white,penwidth=3".getBytes());
					}
	                
	                /////////////////////////////////////
	                //////fill the background of a node
	                /////////////////////////////////////
	            	Object background = getNota(m, stage.stageId, GraphManager.DOT_BACKGROUND, "gray90");
	            	if (null!=background) {
	            		target.write(",style=filled,fillcolor=".getBytes());
	            		target.append(background.toString());
	            	}
	                /////////////////////////////////////
	            	
	                target.write(CLOSEBRACKET_NEWLINE);
	                	                
	            }
	        }
	        
	        /////
	        //DOT_RANK_NAME
	        /////
	        if (null!=ranks) {
	        	
	        	m.cachedRanks = ranks;//keep to use this again
	        	
	        	//TODO: this creates an iterator which needs to be collected, NOTE: find a better way to do this...
		        for (StringBuilder value: ranks.values()) { //removes comma on end
		        	target.append(value, 0, value.length()-1);
		        	target.write(" }\n".getBytes());	        	
		        }
	        }
	        
	        /////
	        //pipes
	        /////
	        
	        int undefIdx = 0;
	        int j = m.pipeIdToPipe.length;
	        while (--j>=0) {
	            Pipe pipe = m.pipeIdToPipe[j];
	            if (null!=pipe) {
	                
	                int producer = getRingProducerId(m, j);
	                int consumer = getRingConsumerId(m, j);
	                
	                //skip all pipes that are gathering monitor data
	                if (consumer<0  ||
	                		((!(GraphManager.getStage(m, consumer) instanceof PipeMonitorCollectorStage))
	                		&& (!(stageForMonitorData(m,GraphManager.getStage(m, consumer))))) 
	                		) {
		                
	                	
	                	byte[] pipeIdBytes = m.pipeDOTNames[pipe.id];
	                	if (null==pipeIdBytes) {
	                	
		                	String pipeId ="";
	                	
			                if (producer>=0) {
			                	pipeId += ("\"Stage"+producer);
			                } else {
			                	pipeId += ("\"Undefined"+undefIdx++);                    
			                }
			                pipeId += ("\" -> ");
			                
			                if (consumer>=0) {
			                	pipeId += ("\"Stage"+consumer);
			                } else {
			                	pipeId += ("\"Undefined"+undefIdx++);
			                }			                
			                
			                pipeIdBytes = m.pipeDOTNames[pipe.id] = pipeId.getBytes();
	                	}

		                
		                boolean showActivePipe = true;
		                int     fanOutGrouping = -1;
		                int     fanInGrouping = -1;
		                
		                if (combineCommonEdges && producer>=0 && consumer>=0) {
		                	
		                			                	
		                	//never combine private topics
		                	if (!"MessagePrivate".equals(Pipe.schemaName(pipe))) {
		                	
		                		int lastPipeId = lastIdForProdPipe(m, pipe, producer);
		                		//combine limit
		                		if (lastPipeId>=0) {
		                			showActivePipe = false;//do not display this pipe alone it is in a group
						    		fanOutGrouping =lastPipeId;
		                		}
		                		
		                		//if not active already grouped so do not group it again.
								if (showActivePipe) {
									
			                		lastPipeId = lastIdForConsPipe(m, pipe, consumer);
			                		//combine limit
			                		if (lastPipeId>=0) {
			                			showActivePipe = false;//do not display this pipe alone it is in a group
							    		fanInGrouping =lastPipeId;
			                		}
								}
		                		
		                	}
		                			
		               			                
			                //disables the pipe back label
	//		                if (GraphManager.hasNota(m, consumer, GraphManager.LOAD_BALANCER)
	//			                	) {
	//			                	if (GraphManager.getOutputPipeCount(m, consumer)>=16) {
	//			                		showLabels = false; //do not show labels for large load balancers
	//			                	}
	//			            }
							
		                }
		                ////////////////////////////////////////
		                
		                if (showActivePipe) {
		                	
		                	
		                	
			                target.write(pipeIdBytes);
			                
			                target.write(LABEL_OPEN);
			                
			                if (pipe.config().showLabels()) {
				                target.write(m.pipeDOTSchemaNames[pipe.id]);
				                		       
				                if (showPipeIdOnTelemetry) {
				                	Appendables.appendValue(target, "#",pipe.id);
				                }
				                
				                if (null!=pipePercentileFullValues) {
				                	target.write(" \n".getBytes());
				                	target.write(pipeFullValues[pipePercentileFullValues[pipe.id]]);
				                	target.write(" ".getBytes());
				                }
				              
				                if (null!=pipeTraffic) {
				                	appendVolume(target, pipeTraffic[pipe.id]);
				                } 
				                target.write(WHITE_SPACE_NL);
				               			                
				                if (null!=msgPerSec) {
				                	target.write(WHITE_SPACE);
				                	fixedSpaceValue(target, msgPerSec[pipe.id], LABEL_TPS);
				                	target.write(WHITE_SPACE);
				                	       	
				                	if (experimentalStdDev) {
					        			//show the messages per pass if they have been recorded.
					        			RunningStdDev writtenFragsStdDev = Pipe.totalWrittenFragmentStdDevperPass(pipe);			
	
					        			if (null!=writtenFragsStdDev && writtenFragsStdDev.sampleCount(writtenFragsStdDev)>0) {
	
					        				double mean = RunningStdDev.mean(writtenFragsStdDev);
					        				double stdDev =  RunningStdDev.stdDeviation(writtenFragsStdDev);
					        				double max =  RunningStdDev.maxSample(writtenFragsStdDev);
					        				
					        				//TODO: reduce compute here by using lookup.....
					        				Appendables.appendValue(target, "", (int)mean,"pc ");
					        				Appendables.appendValue(target, "", (int)stdDev,"std ");
					        				Appendables.appendValue(target, "", (int)max,"max ");	        				
					  
					        			}
				                	}        			
				                }                   
			                    
			                    String pipeMemory = m.pipeDOTConst[pipe.id];
			                    if (null==pipeMemory) {
			                    	pipeMemory = buildPipeConstantText(pipe);
			                    	m.pipeDOTConst[pipe.id] = pipeMemory;
			                    }
			                    target.append(pipeMemory);

			                }
		                    target.write(AQUOTE);

						    int lineWidth = computeLineWidth(pipeTraffic, pipe);
						    
						    if (null!=pipePercentileFullValues) {		                	
						    	int pctFull = pipePercentileFullValues[pipe.id];
						    	if (pctFull>=75) {
						    		target.write(",color=red".getBytes());	    
						    	} else if (pctFull>=40) {
						    		target.write(",color=orange".getBytes());	    
						    	} else {
									target.write(",color=white".getBytes());
								}
						    }
						    Appendables.appendValue(target.append(",penwidth="),lineWidth);
						    target.write(",fontcolor=white".getBytes());

						    target.write(CLOSEBRACKET_NEWLINE);
		                    
		                } 

		                if (!showActivePipe) {
		                	
		                	//System.out.println("a consumer "+consumer);
		                	if (fanOutGrouping>=0 && pipe.id == fanOutGrouping) {
		                
                				target.write(pipeIdBytes);
		                		
                				final int width = GraphManager.getOutputPipeCount(m, producer);
                				int maxPctFull = 0;
                				long sumTraffic = 0;
                				long sumMsgPerSec = 0;
                				long sumFull = 0;
                				String pipeConstMsg = "";
                				int count = 0;
                				int zeroTrafficCount = 0;
                				for(int c=1; c<=width; c++) {
                					
                					Pipe<?> p = GraphManager.getOutputPipe(m, producer, c);
                					
                					//only pick up those pointing to this same consumer and the same pipe schema.
                					if ((GraphManager.getRingConsumerId(m, p.id) == consumer) && Pipe.isForSameSchema(p,pipe) ) {
                					
	                					count++;
	                					if (null!=pipePercentileFullValues) {
	                						//TODO: pull out as constant
	                						int oneMsgSize = 
	                							(100*FieldReferenceOffsetManager.minFragmentSize(Pipe.from(p)))
	                							/
	                							p.sizeOfSlabRing;
	                						//must be 2 since the 1 is used for "any non zero value"
	                						oneMsgSize = Math.max(2, oneMsgSize);
	                							                						
	                						sumFull += (pipePercentileFullValues[p.id]>=oneMsgSize?100:0);
	                						maxPctFull = Math.max(maxPctFull, pipePercentileFullValues[p.id]);
	                					}
	                					
	                					if (null!=pipeTraffic) {
	                						long volume = (long)pipeTraffic[p.id];
	                						sumTraffic += volume;
	                						if (0==volume) {
	                							zeroTrafficCount++;
	                						}
	                					}
	                					if (null!=msgPerSec) {
	                						sumMsgPerSec += (long)msgPerSec[p.id];
	                					}
	                					pipeConstMsg = m.pipeDOTConst[p.id];
	    			                    if (null==pipeConstMsg) {
	    			                    	pipeConstMsg = buildPipeConstantText(p);
	    			                    	m.pipeDOTConst[p.id] = pipeConstMsg;
	    			                    }
                					}
                							
                				}
                				
                				int avgFull = (int)(sumFull/count);
                				                				                				
		                		writeAggregatedPipeLabel(target, pipePercentileFullValues, 
		                				pipeTraffic, msgPerSec,
										width, avgFull, maxPctFull, 
										sumTraffic,	sumMsgPerSec, count, pipeConstMsg, zeroTrafficCount);
		                				
		                	}

		                	if (fanInGrouping>=0 && pipe.id == fanInGrouping) {
		                		//show consolidated single line
		                				                				
                				target.write(pipeIdBytes);
		                		
                				final int width = GraphManager.getInputPipeCount(m, consumer);
                				int maxPctFull = 0;
                				long sumTraffic = 0;
                				long sumMsgPerSec = 0;
                				long sumFull = 0;
                				int count = 0;
                				int zeroTrafficCount = 0;
                				String pipeConstMsg = "";
                				for(int c=1; c<=width; c++) {
                					
                					Pipe<?> p = GraphManager.getInputPipe(m, consumer, c);
                					
                					//only pick up those pointing to this same producer and the same pipe schema.
                					if ((GraphManager.getRingProducerId(m, p.id) == producer) && Pipe.isForSameSchema(p,pipe) ) {
                					
	                					count++;
	                					if (null!=pipePercentileFullValues) {
	                						//TODO: pull out as constant
	                  						int oneMsgSize = 
		                							(100*FieldReferenceOffsetManager.minFragmentSize(Pipe.from(p)))
		                							/
		                							p.sizeOfSlabRing;
	                  					    //must be 2 since the 1 is used for "any non zero value"
	                  						oneMsgSize = Math.max(2, oneMsgSize);
	                  							                  						
	                						sumFull += (pipePercentileFullValues[p.id]>=oneMsgSize?100:0);
	                						maxPctFull = Math.max(maxPctFull, pipePercentileFullValues[p.id]);
	                					}
	                					
	                					if (null!=pipeTraffic) {
	                						long volume = (long)pipeTraffic[p.id];
	                						sumTraffic += volume;
	                						if (0==volume) {
	                							zeroTrafficCount++;
	                						}
	                					}
	                					if (null!=msgPerSec) {
	                						sumMsgPerSec += (long)msgPerSec[p.id];
	                					}	                					
	                					pipeConstMsg = m.pipeDOTConst[p.id];
	    			                    if (null==pipeConstMsg) {
	    			                    	pipeConstMsg = buildPipeConstantText(p);
	    			                    	m.pipeDOTConst[p.id] = pipeConstMsg;
	    			                    }
	                					
                					}
                							
                				}
                				
                				int avgFull = (int)(sumFull/count);
                				                				                				
		                		writeAggregatedPipeLabel(target, pipePercentileFullValues, 
		                				pipeTraffic, msgPerSec,
										width, avgFull, maxPctFull, sumTraffic, 
										sumMsgPerSec, count, pipeConstMsg, zeroTrafficCount);
		                				
		                	}
		                	
    		                
		                }
		                

	                }	          
	            }
	        }	        
	    
            target.write("}\n".getBytes());
   
	}

	int[] cacheLCP;
	
	private static int lastIdForConsPipe(GraphManager m, Pipe pipe, int consumer) {
		
		int key = (Pipe.totalPipes()*consumer)+pipe.id;
		
		if (null==m.cacheLCP || key>=m.cacheLCP.length) {
			m.cacheLCP = new int[Math.max(m.stageCounter.get()*Pipe.totalPipes(),key+1)];
			Arrays.fill(m.cacheLCP, -3);
		}
		
		if (m.cacheLCP[key]!=-3) {
			return m.cacheLCP[key];
		}
		
		int lastPipeId=-1;
		int c = 0;
		{
			final int inputPipeCount = GraphManager.getInputPipeCount(m, consumer);
			for(int p=1;p<=inputPipeCount;p++) {
				
				Pipe peerPipe = GraphManager.getInputPipe(m, consumer, p);
				
				if (Pipe.isForSameSchema(pipe,peerPipe) //and same consumer
					&& GraphManager.getRingProducerId(m, pipe.id)==GraphManager.getRingProducerId(m, peerPipe.id)	
						) {
					c++;
					lastPipeId = peerPipe.id;
					
				}
			}
		}
		if (c==1) {
			lastPipeId = -1;//do not group a single pipe.
		}
		m.cacheLCP[key] = lastPipeId;
		return lastPipeId;
	}

	int[] cacheLPP;
	
	private static int lastIdForProdPipe(GraphManager m, Pipe pipe, int producer) {
		int key = (Pipe.totalPipes()*producer)+pipe.id;
		
		if (null==m.cacheLPP || key>=m.cacheLPP.length) {
			m.cacheLPP = new int[Math.max(m.stageCounter.get()*Pipe.totalPipes(),key+1)];
			Arrays.fill(m.cacheLPP, -3);
		}
		
		if (m.cacheLPP[key]!=-3) {
			return m.cacheLPP[key];
		}
		
		int lastPipeId=-1;
		int c = 0;
		{
			final int outputPipeCount = GraphManager.getOutputPipeCount(m, producer);
			for(int p=1;p<=outputPipeCount;p++) {
				
				Pipe peerPipe = GraphManager.getOutputPipe(m, producer, p);
				
				if (Pipe.isForSameSchema(pipe,peerPipe) //and same consumer
					&& GraphManager.getRingConsumerId(m, pipe.id)==GraphManager.getRingConsumerId(m, peerPipe.id)	
						) {
					c++;
					lastPipeId = peerPipe.id;
				}
			}
		}
		if (c==1) {
			lastPipeId = -1;//do not group a single pipe.
		}
		m.cacheLPP[key] = lastPipeId;
		return lastPipeId;
	}

	private static boolean allTheSame(GraphManager m, int producer, int outputPipeCount) {
		boolean allTheSame = true;
		int lastConsumer = -1;
		
		for(int w=1; w<=outputPipeCount; w++) {
			int consumerId = GraphManager.getRingConsumerId(m, 								
						GraphManager.getOutputPipe(m, producer, w).id
					);
			
			if (-1 == lastConsumer) {
				lastConsumer = consumerId;
			} else {
				if (consumerId != lastConsumer) {
					allTheSame = false;
					break;
				}
			}
		}
		return allTheSame;
	}

	private static void writeAggregatedPipeLabel(AppendableByteWriter<?> target, int[] pipePercentileFullValues,
			long[] pipeTraffic, int[] msgPerSec, final int width, 
			int allPctUsed, int maxPctFull, long sumTraffic, long sumMsgPerSec,
			int count, String constMsg, int zeroTrafficCount) {
		target.write(LABEL_OPEN);
		
		Appendables.appendValue(target, count);
		
		if (null!=constMsg && constMsg.length()>0) {
			target.append(" ");
			target.append(constMsg);
		}
		
		target.append(" Pipes\n");
		
		if (zeroTrafficCount>0) {
			Appendables.appendValue(target, zeroTrafficCount);
			target.append(" never used\n");
		}
		
		if (null!=pipePercentileFullValues) {		
			target.append(" M");
			target.write(pipeFullValues[maxPctFull]);
			target.append(" A");
			target.write(pipeFullValues[allPctUsed]);
			target.append("\n");			
		}
		//right here.
		//Full:00% //need Full:00.00% for aggrigate on its own line.
		
		//Appendables.appendFixedDecimalDigits(target.append("Full:"), k, k<=99?10:100).append("%\n");
		
		
		if (null!=pipeTraffic) {
			appendVolume(target, sumTraffic);
			target.write(WHITE_SPACE_NL);
		} 
		if (null!=msgPerSec) {
			fixedSpaceValue(target, sumMsgPerSec, LABEL_TPS);
		}
		
		target.write(AQUOTE);
		
		int lineWidth = 10;
		
		if (null!=pipePercentileFullValues) {          	
			int triggerSource = Math.max(allPctUsed,maxPctFull);
					
			if (triggerSource>=80) {
				target.append(",color=red");	    
			} else if (triggerSource>=60) {
				target.append(",color=orange");	    
			} else {
				target.append(",color=\"#b2b2b2\""); //replaced gray30 with the inverse of it, which is #B2B2B2
			}
		}
		
		Appendables.appendValue(target.append(",penwidth="),lineWidth);
		target.append(",fontcolor=white");
		
		target.write(CLOSEBRACKET_NEWLINE);
	}

	private static boolean isSameDestination(GraphManager m, int producer, int a, int b) {
		
		Pipe<?> outputPipe1 = GraphManager.getOutputPipe(m, producer, a);
		Pipe<?> outputPipe2 = GraphManager.getOutputPipe(m, producer, b);			
		int con1 = GraphManager.getRingConsumerId(m, outputPipe1.id);	
		int con2 = GraphManager.getRingConsumerId(m, outputPipe2.id);		
		return (con1==con2 
				&& Pipe.isForSameSchema(outputPipe1, outputPipe2)
				);

	}
	
	private static boolean isSameSource(GraphManager m, int consumer, int a, int b) {
		
		Pipe<?> inputPipe1 = GraphManager.getInputPipe(m, consumer, a);
		Pipe<?> inputPipe2 = GraphManager.getInputPipe(m, consumer, b);			
		int prod1 = GraphManager.getRingProducerId(m, inputPipe1.id);	
		int prod2 = GraphManager.getRingProducerId(m, inputPipe2.id);		
		//System.out.println(prod1+" "+prod2+"  "+inputPipe1+" "+inputPipe2);
		return (prod1==prod2 && Pipe.isForSameSchema(inputPipe1, inputPipe2));

	}

	private static void appendVolume(AppendableByteWriter<?> target, long traf) {
		if (traf>9999) {
			Appendables.appendValue(target.append("Vol:"), traf);
		} else {
			if (traf>=0) {
				Appendables.appendFixedDecimalDigits(target.append("Vol:"), traf, 1000);
			} else {
				new Exception("bad traff: "+traf).printStackTrace();
			}
		}
	}

	private static void fixedSpaceValue(AppendableByteWriter<?> target, long value, byte[] msgPerSeclabel) {
		if (value<10_000) {
			//use x.xxx places (5)
			Appendables.appendDecimalValue(target, value, (byte)-3).write(msgPerSeclabel);			          
		} else {		
			if (value<100_000) {
				//use xx.xx places (5)
				Appendables.appendDecimalValue(target, value/10, (byte)-2).write(msgPerSeclabel);			          
			} else {
				if (value<1000_000) {
		    		//use xxx.x places (5)
		    		Appendables.appendDecimalValue(target, value/100, (byte)-1).write(msgPerSeclabel);			          
		    	} else {
		    		if (value<10_000_000) {
		    			Appendables.appendFixedDecimalDigits(target, value/1000, 1_000).write(msgPerSeclabel);
		    		} else {
		    			long intValue = value/1000;
						//if (intValue>1000) {
							AppendableByteWriter<?> t = Appendables.appendValue(target, intValue/1000);
							t.writeByte((int)'k');
						    t.write(msgPerSeclabel);
						//} else {
						//	Appendables.appendValue(target, intValue).write(msgPerSeclabel);
						//}
		    		}
		    	}
			}
		}
	}

	private static boolean mustShowOnTelemetry(GraphManager m, PronghornStage stage) {
		return null!=stage 
				&& !(stage instanceof PipeMonitorCollectorStage) 
				&& !(stage instanceof PipeMonitorStage)
				&& !stageForMonitorData(m,stage);
	}

	private static void writeElapsed(AppendableByteWriter<?> target, long valueNS) {
		target.write(ELAP);
		
		if (valueNS<7_000) {
			Appendables.appendFixedDecimalDigits(target, valueNS, 1000).write(NS);
		} else if (valueNS<7_000_000){
			Appendables.appendFixedDecimalDigits(target, valueNS/1_000, 1000).write(MICROS);
		} else if (valueNS<7_000_000_000L){
			Appendables.appendFixedDecimalDigits(target, valueNS/1_000_000, 1000).write(MS);				
		} else if (valueNS<90_000_000_000L){
			Appendables.appendFixedDecimalDigits(target, valueNS/1_000_000_000L, 100).write(SEC);
		} else if (valueNS<(120L*60_000_000_000L)){
			Appendables.appendFixedDecimalDigits(target, valueNS/60_000_000_000L, 100).write(MIN);
		} else {
			Appendables.appendValue(target, valueNS/(60L*60_000_000_000L)).write(HR);
		}
	
	}

	private static int computeLineWidth(long[] traffic, Pipe pipe) {
		int lineWidth = 1; //default
		if (null!=traffic) {
			long trafficCount = traffic[pipe.id];
			//compute the line width.
			int bitsUsed = 64-Long.numberOfLeadingZeros(trafficCount);		                	
			lineWidth = (0==bitsUsed)? 1 : 2 +(bitsUsed>>3);			                	
		}
		return lineWidth;
	}

	private static void populateRanks(GraphManager m, Map<Object, StringBuilder> ranks, PronghornStage stage,
			String stageId) {
		if (ranks!=null && m.cachedRanks==null) {
			//thes rank keys are cached
			Object rankKey = getNota(m, stage.stageId, GraphManager.DOT_RANK_NAME, null);
			if (rankKey!=null) {
				
				//{ rank=same, b, c, d }
				StringBuilder b = ranks.get(rankKey);
				if (null==b) {
					b = new StringBuilder("{ rank=same");
					ranks.put(rankKey, b);
				}
				
				b.append(" \"").append(stageId).append("\",");
				
			}
		}
	}

	private static byte[] buildStageDOTRate(GraphManager m, PronghornStage stage) {
		Object rate = getNota(m, stage.stageId, GraphManager.SCHEDULE_RATE, null);
		if (null!=rate) {
			StringBuilder builder = new StringBuilder();
			builder.append("\n Rate:");
			Appendables.appendNearestTimeUnit(builder, ((Number)rate).longValue());
			return m.stageDOTRate[stage.stageId] = builder.toString().getBytes();
		}
		return null;
	}

	private static byte[] buldStageDOTName(GraphManager m, PronghornStage stage) {
		//TODO: re-visit to eliminate GC.
		String stageDisplayName = stage.toString()
				      .replace("Stage","").trim()
				      .replace(" ", "\n")+"\n";

		if (showThreadIdOnTelemetry) {
			Object group = GraphManager.getNota(m, stage.stageId, GraphManager.THREAD_GROUP, null);
			if (null!=group) {
				stageDisplayName+=(" Thread:"+group.toString()+"\n");
			}
		}
		
		return m.stageDOTNames[stage.stageId] = stageDisplayName.getBytes();
	}

	private static String buildPipeConstantText(Pipe pipe) {
		String pipeMemory;
		pipeMemory = "";
		
		/////////////////////////////////////////
		//compute the min and max count of messages that can be on this pipe at any time
		FieldReferenceOffsetManager from = Pipe.from(pipe);
		
		int maxMessagesOnPipe = -1;
		int minMessagesOnPipe = -1;
		if (null != from) {
			maxMessagesOnPipe = pipe.sizeOfSlabRing/FieldReferenceOffsetManager.minFragmentSize(from);
			minMessagesOnPipe = pipe.sizeOfSlabRing/FieldReferenceOffsetManager.maxFragmentSize(from);           
		}
		///////////////////////////////
		///////////////////////////////
		
		if (showMessageCountRangeOnTelemetry && minMessagesOnPipe>=0) {
			
			if (minMessagesOnPipe==maxMessagesOnPipe) {
				pipeMemory += (" ["+minMessagesOnPipe+"msg]");
			} else {
				pipeMemory += (" ["+minMessagesOnPipe+"-"+maxMessagesOnPipe+"msgs]");
			}  

		}
		
		
		
		long bytesAllocated = Pipe.estBytesAllocated(pipe);
		if (bytesAllocated > (1L<<31)) {
			pipeMemory += Long.toString(bytesAllocated>>>30)+"g";
		} else if (bytesAllocated > (1L<<21)) {
			pipeMemory += Long.toString(bytesAllocated>>>20)+"m";
		} else if (bytesAllocated > (1L<<11)) {
			pipeMemory += Long.toString(bytesAllocated>>>10)+"k";
		} else {
			pipeMemory += Long.toString(bytesAllocated)+"b";
		}
		return pipeMemory;
	}

    private static float computeWeightBetweenStages(GraphManager m, int consumerStage, int producerStage) {
        float weight = 0f;
        Pipe[] pipes = m.pipeIdToPipe;
        int p = pipes.length;
        while (--p >= 0) {
            if (null != pipes[p]) {
                if (producerStage == GraphManager.getRingProducerId(m, pipes[p].id) &&
                    consumerStage == GraphManager.getRingConsumerId(m, pipes[p].id)) {
                    weight += 2;
                    
                } else if (consumerStage == GraphManager.getRingProducerId(m, pipes[p].id) &&
                           producerStage == GraphManager.getRingConsumerId(m, pipes[p].id)) {
                    weight += 2;
                    
                }
            }
        }
        return weight;
    }
	
	
	public static void logOutputs(Logger log, GraphManager m, PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {					
			log.error("Output Queue :{}",m.pipeIdToPipe[ringId]);				
		}	
	}

	public static void logInputs(Logger log, GraphManager m,	PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			log.error("Input Queue :{}",m.pipeIdToPipe[ringId]);					
		}				
		
	}

	public static boolean isInputsEmpty(GraphManager m, PronghornStage stage) {
		 return isInputsEmpty(m, stage.stageId);
	}

	public static boolean isInputsEmpty(GraphManager m, int stageId) {
		int ringId;
	     int idx = m.stageIdToInputsBeginIdx[stageId];
	     while (-1 != (ringId=m.multInputIds[idx++])) { 
	         
	         if (Pipe.contentRemaining(m.pipeIdToPipe[ringId])>0) {
	             return false;
	         }                
	     }               
	     return true;
	}
	
	/**
	 * Initialize the buffers for the input rings of this stage and
	 * Block until some other code has initialized the output rings. 
	 * 
	 * @param m
	 * @param stageId
	 */
	public static void initAllPipes(GraphManager m, int stageId) {
		int idx;
		initInputPipesAsNeeded(m, stageId);
		//Does not return until some other stage has initialized the output rings
		int pipeId;
		idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (pipeId=m.multOutputIds[idx++])) {
		    
		    
		    try {
    		    //double check that this was not built wrong, there must be a consumer of this ring or it was explicitly initialized
		        int consumerId = getRingConsumerId(m, pipeId);
		        if (consumerId < 0 && !Pipe.isInit(getPipe(m, pipeId))) {
		            
		            String schemaName = Pipe.schemaName(m.pipeIdToPipe[pipeId]);
		
		            int producerId = getRingProducerId(m, pipeId);
		            if (producerId<0) {
		                throw new UnsupportedOperationException("Can not find stage consuming Pipe<"+schemaName+"> #"+pipeId+" "+CHECK_GRAPH_CONSTRUCTION);		                
		            } else {
		                PronghornStage prodStage = getRingProducer(m, pipeId);
		                
		                throw new UnsupportedOperationException("Can not find stage consuming Pipe<"+new String(m.pipeDOTSchemaNames[pipeId])+"> #"+pipeId+" Which is produced by stage "+prodStage+". "+CHECK_GRAPH_CONSTRUCTION);
		                
		            }
		        }
		    } catch (ArrayIndexOutOfBoundsException aiobe) {
		        if ("-1".equals(aiobe.getMessage())) {
		            throw new UnsupportedOperationException("No consumer for pipe "+pipeId);
		        } else {
		            throw new RuntimeException(aiobe);
		        }
		    }

		    if (!stageForMonitorData(m, getStage(m, stageId))) {
		    	int monitorStartupTimeout = 60_000; //1 min timeout for startup
		    	Thread.yield();//let the Pipe get initialized if possible.		    	
			    //blocking wait on the other stage to init this pipe, required for clean startup only.
			    long timeout = System.currentTimeMillis()+monitorStartupTimeout;
				while (!Pipe.isInit(m.pipeIdToPipe[pipeId])) {
					try {
						Thread.sleep(2);//extra sleep for startup of monitoring.
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
					if (System.currentTimeMillis()>timeout) {
						throw new RuntimeException("Check Graph, unable to startup "+GraphManager.getStage(m, stageId)+" due to output "+m.pipeIdToPipe[pipeId]+" consumed by "+getRingConsumer(m,m.pipeIdToPipe[pipeId].id));
					}
				}				
			}
		}	
		
	}

	public static void initInputPipesAsNeeded(GraphManager m, int stageId) {
		int pipeIdIn;
		int idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (pipeIdIn=m.multInputIds[idx++])) {
			if (!Pipe.isInit(m.pipeIdToPipe[pipeIdIn])) {
				m.pipeIdToPipe[pipeIdIn].initBuffers();		
			}
		}
	}
	
	public static void reportError(GraphManager graphManager, final PronghornStage stage, Throwable t, Logger logger) {
          String msg = t.getMessage();
          if (null != msg && msg.contains(CHECK_GRAPH_CONSTRUCTION)) {
              logger.error(msg);
          } else {                
              if (null==stage) {
                  logger.error("Stage was never initialized");
              } else {              
                  logger.error("Unexpected error in {} ", stage, t);       
                  GraphManager.shutdownNeighborRings(graphManager, stage);
              }
          }
    }  

	public static Pipe<PipeMonitorSchema>[] attachMonitorsToGraph(GraphManager gm, Long monitorRate, PipeConfig<PipeMonitorSchema> ringBufferMonitorConfig) {

		//////////////////////////
		//TODO: we need a limit, like 2000 or so, if we need to monitor more than this many
		//      we must use multiple PipeMonitorStage instances instead of one
		//////////////////////////
		
		int j = gm.pipeIdToPipe.length;
		int count = 0;
		while (--j>=0) {
			if (null!=gm.pipeIdToPipe[j] &&
					((!ringHoldsMonitorData(gm, gm.pipeIdToPipe[j])))
							) {
				count++;
			}
		}
		if (0==count) {
			throw new UnsupportedOperationException("Nothing to monitor, move this call down to after graph is constructed.");
		}
		
		Pipe[] monBuffers = new Pipe[count];
		Pipe[] observedBuffers = new Pipe[count];
		int monBufIdx = 0;
		j = gm.pipeIdToPipe.length;
		while (--j>=0) {
			
			Pipe ringBuffer = gm.pipeIdToPipe[j];
			//Do not monitor those rings that are part of other monitoring networks.
			if (null!=ringBuffer && 
					((!ringHoldsMonitorData(gm, ringBuffer)))				
				) {

				observedBuffers[monBufIdx] = ringBuffer;
				monBuffers[monBufIdx] = new Pipe<PipeMonitorSchema>(ringBufferMonitorConfig);
				
				monBufIdx++;
				
			}
		}
		
		PipeMonitorStage stage = new PipeMonitorStage(gm, observedBuffers, monBuffers);
		
		stage.setNotaFlag(PronghornStage.FLAG_MONITOR);
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, monitorRate, stage);
				
		return monBuffers;	
	}

	private static boolean ringHoldsMonitorData(GraphManager gm, Pipe ringBuffer) {
		if (!monitorAll) {
			int producer = GraphManager.getRingProducerId(gm, ringBuffer.id);
			if (producer>=0) {
				return GraphManager.getStage(gm, producer).isMonitor();
			}
		}
		return false;
		
	}
	
    private static boolean stageForMonitorData(GraphManager gm, PronghornStage stage) {
    	if (!monitorAll) {    		
    		return stage.isMonitor();
		}
    	return false;
    	
    }

	public static void enableBatching(GraphManager gm) {
		int j = gm.pipeIdToPipe.length;
		while (--j>=0) {
			Pipe ring = gm.pipeIdToPipe[j];
			//never enable batching on the monitor rings
			if (null!=ring && !ringHoldsMonitorData(gm, ring) ) {
				
				int ringId1 = ring.id;
				int stageId1 = GraphManager.getRingConsumerId(gm, ringId1);
				if (stageId1>=0) {
					if (PronghornStage.supportsBatchedRelease(gm.stageIdToStage[stageId1])) { 
						Pipe.setMaxReleaseBatchSize(ring);
					}
				}
				
				int ringId = ring.id;
				int stageId = GraphManager.getRingProducerId(gm, ringId);
				if (stageId>=0) {
					if (PronghornStage.supportsBatchedPublish(gm.stageIdToStage[stageId])) {
						Pipe.setMaxPublishBatchSize(ring);
					}
				}
			}
		}
	}

//	public static String getPipeName(GraphManager gm, Pipe pipe) {
//		
//	    final int ringId = pipe.id;
//	    String consumerName = "UnknownConsumer";
//	    {
//            int stageId = getRingConsumerId(gm, ringId);
//            if (stageId>=0) {
//                PronghornStage consumer = gm.stageIdToStage[stageId];
//                consumerName = extractName(gm, consumer)+"#"+stageId;
//            }
//	    }
//	    String producerName = "UnknownProducer";
//	    {
//            int stageId = getRingProducerId(gm, ringId);
//            if (stageId>=0) {                
//                PronghornStage producer = gm.stageIdToStage[stageId];                
//                producerName = extractName(gm, producer)+"#"+stageId;  
//            }
//	    }
//	    StringBuilder builder = new StringBuilder();
//	    builder.append(producerName).append('-');
//	    Appendables.appendValue(builder, pipe.id);
//	    builder.append('-').append(consumerName);
//	    return builder.toString();
//	}
//
//	private static String extractName(GraphManager gm, PronghornStage stage) {
//		return getNota(gm, stage, STAGE_NAME, stage.getClass().getSimpleName()).toString();
//	}

	/**
	 * Start with ordinal selection of input stages then ordinal selection of each output ring there after.
	 * TODO: do generic return that extends pronghornStage
	 * @param m
	 * @param path
	 */
	public static PronghornStage findStageByPath(GraphManager m, int ... path) {
		
		int ordinal = path[0];
		int i = 0;
	    int limit = m.stageIdToStage.length;
		while (i<limit) {
			if (null!=m.stageIdToStage[i]) {				
				//an input stage is one that has no input ring buffers
				if (-1 == m.multInputIds[m.stageIdToInputsBeginIdx[m.stageIdToStage[i].stageId]]) {
					if (--ordinal<=0) {
						//starting from 1 find this path
						return findStageByPath(m, m.stageIdToStage[i], 1, path);
					}
				}
			}
			i++;
		}	
		throw new UnsupportedOperationException("Unable to find ordinal input stage of "+path[0]);
	}

	private static PronghornStage findStageByPath(GraphManager m, PronghornStage stage, int idx, int[] path) {
		if (idx>=path.length) {
			return stage;
		}
		return findStageByPath(m,getRingConsumer(m, getOutputPipe(m,stage,path[idx]).id),1+idx,path);
	}

	//when batching is used we need to flush outstanding writes before yield
	public static void publishAllWrites(GraphManager m, PronghornStage stage) {
	    
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {
		    if (Pipe.getPublishBatchSize(m.pipeIdToPipe[ringId])>0) {
		        Pipe.publishAllBatchedWrites(m.pipeIdToPipe[ringId]);	
		    }
		}
		
	}

	@Deprecated
    public void blockUntilStageBeginsShutdown(PronghornStage stageToWatch) {
		blockUntilStageHasTerminated(stageToWatch);
    }

    public void blockUntilStageHasTerminated(PronghornStage stageToWatch) {
		blockUntilStageTerminated(this, stageToWatch);
	}
	
    private String 			telemetryHost = null;
    private int    			telemetryPort = -1; // <0 no telmetry, 0 log telemetry, >0 port for telemetry server
    
    //TODO: do not enable until the index.html can use https for its call back...
    private TLSCertificates telemetryCert = null;//TLSCertificates.defaultCerts;
    
    public String enableTelemetry(String host, int port) {
    	telemetryHost = port>0?NetGraphBuilder.bindHost(host):null;
    	telemetryPort = port;
    	
    	return host;
	}
    
    public String enableTelemetry(int port) {
    	telemetryHost = port>0?NetGraphBuilder.bindHost(telemetryHost):null;
    	telemetryPort = port;
    	
    	return telemetryHost;
	}

	public static void recordThreadGroup(PronghornStage stage, final int group, final GraphManager graphManager) {
		//logger.info("recorded group for {} {}",group,stage);
		
		addNota(graphManager, THREAD_GROUP, group, stage);
		buldStageDOTName(graphManager, stage);
	}

	public static void spinLockUntilStageOfTypeStarted(GraphManager gm, Class<?> stageClass) {
        boolean isStarted;
        do {
            isStarted = true;
            Thread.yield();
            PronghornStage[] stages = allStagesByType(gm, stageClass);
            int s = stages.length;
            while (--s>=0) {
                isStarted &= isStageStarted(gm, stages[s].stageId);
            }
        } while (!isStarted);
    }

    public static void blockUntilStageTerminated(GraphManager gm, PronghornStage stageToWatch) {
        //keep waiting until this stage starts it shut down or completed its shutdown,
        //eg return on leading edge as soon as we detect shutdown in progress..
        while (!  (isStageShuttingDown(gm, stageToWatch.stageId)||isStageTerminated(gm, stageToWatch.stageId)) ) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static boolean blockUntilStageTerminated(GraphManager gm, PronghornStage stageToWatch, long timeoutMS) {
        //keep waiting until this stage starts it shut down or completed its shutdown,
        //eg return on leading edge as soon as we detect shutdown in progress..
        while (--timeoutMS>=0 && (!  (isStageShuttingDown(gm, stageToWatch.stageId)||isStageTerminated(gm, stageToWatch.stageId))) ) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            	Thread.currentThread().interrupt();
            	return true;
            }
        }
        return timeoutMS>=0;
    }
    

    public static PronghornStage[] allStagesByType(GraphManager graphManager, Class<?> stageClass) {
    	 //TODO: rewrite and simple recursive stack unroll to eliminate the duplication of the code here. see pipesOfType
        int count = 0;
        int s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null!=stage && stage.getClass().isAssignableFrom(stageClass)) {
                count++;
            }
        }
        
        PronghornStage[] stages = new PronghornStage[count];
        s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null != stage && stage.getClass().isAssignableFrom(stageClass)) {
                stages[--count] = stage;
            }
        }
        return stages;
    }

    public static boolean isAllPipesEmpty(GraphManager graphManager) {        
        Pipe[] pipes = graphManager.pipeIdToPipe;
        int p = pipes.length;
        while (--p >= 0) {
            if (null != pipes[p]) {
                if (Pipe.contentRemaining(pipes[p]) > 0) {   
                    return false;
                }
            }
        }
        return true;
    }

    
    private static final int defaultDurationWhenZero = 1;
    private static AtomicInteger totalZeroDurations = new AtomicInteger();
     
    //returns true when the run time was positive and > 0
	public static boolean accumRunTimeNS(GraphManager graphManager, int stageId, long duration, long now) {

		if (duration>0) {
			accumPositiveTime(graphManager, stageId, duration, now);
			return true;
		} else {
			accumWhenZero(graphManager, stageId, duration);
			return false;
		}
	}

	private static final long AVG_BITS = 7;
	
	
	private static void accumPositiveTime(GraphManager graphManager, int stageId, long duration, long now) {
		long[] stageLastTimeNsLocal = graphManager.stageLastTimeNs;
		long last = stageLastTimeNsLocal[stageId];
		
		if (last>0 && last<now) {
			accumDuration(graphManager, stageId, duration, now-last); 

			//this total duration is also used as the baseline for the histogram of elapsed time
			graphManager.stageRunNS[stageId] += duration;
		} 
		
		//we have the running pct since startup but we need a recent rolling value.
		stageLastTimeNsLocal[stageId] = now;
		
	}

		
	private static void accumDuration(GraphManager graphManager, int stageId, long duration, long fullPeriod) {
		if (recordElapsedTime) {
			
			buildHistogramsAsNeeded(graphManager, stageId);
			ElapsedTimeRecorder.record(graphManager.stageElapsed[stageId], duration);

		}		
		
		long newPct = ( (100_000L*(duration)) / (fullPeriod) );		
		setWorkPct(graphManager, stageId, newPct);
	}

	private static void setWorkPct(GraphManager graphManager, int stageId, long newPct) {
		int oldPct = graphManager.stageWrkPct[stageId];
		
		graphManager.stageWrkPct[stageId] = (int) 
				(0==oldPct ? newPct: (newPct + ( ((((long)oldPct)<<AVG_BITS)-((long)oldPct))  )) >> AVG_BITS );
	}
	
	public static void recordNoWorkDone(GraphManager graphManager, int stageId) {
		setWorkPct(graphManager, stageId, 0);
	}

	private static void accumWhenZero(GraphManager graphManager, int stageId, long duration) {
		PronghornStage stage = getStage(graphManager, stageId);
		
		if ((stage instanceof PipeCleanerStage) ||
			(stage instanceof ReplicatorStage) ) {
			
			//these can be very fast and should not be logged.
			
		} else {
		
			int x = totalZeroDurations.incrementAndGet();
							
			if (Integer.numberOfLeadingZeros(x-1)!=Integer.numberOfLeadingZeros(x)) {
				if (duration<0) {
					logger.info("Bad duration {}",duration);
				} else {
					logger.trace("Warning: the OS has measured stages taking zero ms {} times. "
						       + "Most recent case is for {}. ", x, stage);
				}
			}
			
			graphManager.stageRunNS[stageId] += defaultDurationWhenZero;
		}
	}

	private static void buildHistogramsAsNeeded(GraphManager graphManager, int stageId) {
		if (stageId >= graphManager.stageElapsed.length) {
			int maxArray = 1+graphManager.stageCounter.get(); //largestId plus 1
			//Does this need to grow??
			
			ElapsedTimeRecorder[] newHE = new ElapsedTimeRecorder[maxArray];
			int i = maxArray;
			while (--i>=0) {				
				newHE[i] = new ElapsedTimeRecorder();
			}
			graphManager.stageElapsed = newHE;
		}
	}

	public static void accumRunTimeAll(GraphManager graphManager, int stageId) {		
		graphManager.stageRunNS[stageId] = -1; //flag for 100%
	}

	private long[] stageBitMap;
	private StageDetector loopDetector;
	
	
	public static synchronized int isStageInLoop(GraphManager graphManager, final int stageId) {
			
		if (null == graphManager.loopDetector) {
			graphManager.loopDetector = new StageDetector();
		}
		graphManager.loopDetector.setTarget(graphManager, stageId);
		
		int c =	GraphManager.getOutputPipeCount(graphManager, stageId);
		for(int i = 1; i<=c; i++) {			
			int outPipeId = GraphManager.getOutputPipe(graphManager, stageId, c).id;
			visitDownstream(GraphManager.getRingConsumerId(graphManager, outPipeId), 
							graphManager, 
							cleanStageBitMap(graphManager), 
							0, 
							graphManager.loopDetector);
			if ( graphManager.loopDetector.wasDetected()) {
				return graphManager.loopDetector.foundProducerStageId(); //will be -1 when no prodcuer was found.
			}
		}
		return -2; //This is not in a loop
	}

	private static long[] cleanStageBitMap(GraphManager graphManager) {
		if (null==graphManager.stageBitMap) {
			int maxStages = graphManager.stageCounter.get();
			int bits = (int)Math.ceil(Math.log(maxStages)/Math.log(2));
	    	int pow2Stages = 1<<bits;
	    	int longStages = pow2Stages>>6;// divide by 64
	    	graphManager.stageBitMap = new long[longStages+1];//one extra for last long of values
		}
		
		Arrays.fill(graphManager.stageBitMap, 0);
		
		long[] map = graphManager.stageBitMap;
		return map;
	}

	private static void visitDownstream(int stageId, GraphManager graphManager, long[] bitMap, int depth, GraphVisitor visitor) {
		assert(stageId<=graphManager.stageCounter.get()) : "This stage id is out of range";
		if (stageId<0) {
			//nothing to visit
			return;
		}
		
		int mapIndex = stageId>>6;//divide by 64
		int mapBit   = 1<<(stageId&0x3F);
	    if ((bitMap[mapIndex]&mapBit)!=0) {
			return;//we have encountered some place we have already been.
		} else {
			//mark this as a new place we have been
			bitMap[mapIndex] |= mapBit;
			//now call everyone downstream
			
			//visit children only if visitor returns true
			if (visitor.visit(graphManager, stageId, depth)) {
				int c =	GraphManager.getOutputPipeCount(graphManager, stageId);
				for(int i = 1; i<=c; i++) {		
					int outPipeId = GraphManager.getOutputPipe(graphManager, stageId, c).id;
					visitDownstream(GraphManager.getRingConsumerId(graphManager, outPipeId), graphManager, bitMap, depth+1, visitor);
				}
			}
		}
		
	}

	RunningStdDev stdDevPipes = null;
	
	public static RunningStdDev stdDevPipesPerStage(GraphManager m) {
		   
		   if (null == m.stdDevPipes) {
			 
			   m.stdDevPipes = new RunningStdDev();
			   int i = m.stageIdToStage.length;
		       while (--i>=0) {
		           PronghornStage stage = m.stageIdToStage[i];
		           if (null!=stage) {
		        	  
		        	  int sample =  
		        	  getInputPipeCount(m, stage.stageId)+
		        	  getOutputPipeCount(m, stage.stageId);
		        	  
		        	  RunningStdDev.sample(m.stdDevPipes, sample);
		        	  
		           }
		       }		       
		   }
	      return m.stdDevPipes;
	}

    public static void shutdownStage(GraphManager gm, PronghornStage stage) {
    	stage.shutdown();
    }
    
    public static void monitorPipe(GraphManager gm, int pipeId, Appendable target) {
    	    	    	
    	ConsoleJSONDumpStage.newInstance(gm, PipeMonitor.addMonitor(getPipe(gm, pipeId)), target);
    	
    }
    
    public static void monitorPipe(GraphManager gm, int pipeId, File targetFile) {
    	
		try {
			final PrintWriter target = new PrintWriter(targetFile);
			new ConsoleJSONDumpStage(gm, PipeMonitor.addMonitor(getPipe(gm, pipeId)), target) {
				@Override
				public void shutdown() {
					super.shutdown();
					target.close();
				}
			};
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		
    }

	public static void appendInputs(GraphManager graphManager, Appendable target, PronghornStage pronghornStage) {
		
		int c = GraphManager.getInputPipeCount(graphManager, pronghornStage.stageId);
		for(int i = 1; i<=c; i++) {
			int id = GraphManager.getInputPipe(graphManager, pronghornStage.stageId, i).id;
			Appendables.appendValue(target, i==1?"":" ,", id);
		}
		
	}
	
	public static void appendOutputs(GraphManager graphManager, Appendable target, PronghornStage pronghornStage) {
		
		int c = GraphManager.getOutputPipeCount(graphManager, pronghornStage.stageId);
		for(int i = 1; i<=c; i++) {
			int id = GraphManager.getOutputPipe(graphManager, pronghornStage.stageId, i).id;
			Appendables.appendValue(target, i==1?"":" ,", id);
		}
		
	}

	public static void addPublishFromListener(GraphManager graphManager, 
			                              PronghornStage pronghornStage, 
			                              PipePublishListener listener) {
		int c = getOutputPipeCount(graphManager, pronghornStage.stageId);
		for(int i=1; i<=c; i++) {
			Pipe.addPubListener(getOutputPipe(graphManager, pronghornStage.stageId, i), listener);
		}
	}

	public static void removePublishFromListener(GraphManager graphManager, 
			PronghornStage pronghornStage,
			PipePublishListener listener) {
		int c = getOutputPipeCount(graphManager, pronghornStage.stageId);
		for(int i=1; i<=c; i++) {
			Pipe.removePubListener(getOutputPipe(graphManager, pronghornStage.stageId, i), listener);
		}
	}
	

}
