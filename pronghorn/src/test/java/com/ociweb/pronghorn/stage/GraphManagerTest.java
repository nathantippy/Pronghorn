package com.ociweb.pronghorn.stage;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class GraphManagerTest {

    @Test
    public void notaTests() {
    
        final GraphManager gm = new GraphManager();
        
        Pipe rb1 = new Pipe(new PipeConfig(RawDataSchema.instance));
        rb1.initBuffers();
        
        Pipe rb2 = new Pipe(new PipeConfig(RawDataSchema.instance));
        rb2.initBuffers();
        
        PronghornStage a = new SimpleOut(gm,rb1); 
        GraphManager.addNota(gm, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, a);
        PronghornStage b = new SimpleInOut(gm,rb1,rb2); 
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 20000, b);
        PronghornStage c = new SimpleIn(gm,rb2); 
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10000, c);
        
        
        assertEquals(a, GraphManager.getStageWithNotaKey(gm, GraphManager.UNSCHEDULED, 1));
        
        try{
         GraphManager.getStageWithNotaKey(gm, GraphManager.UNSCHEDULED, 2);
         fail("should not have found");
        } catch (UnsupportedOperationException uoe) {
            //ok
        }
                
        assertEquals(2, GraphManager.countStagesWithNotaKey(gm, GraphManager.SCHEDULE_RATE));
        assertEquals(1, GraphManager.countStagesWithNotaKey(GraphManager.cloneStagesWithNotaKeyValue(gm, GraphManager.SCHEDULE_RATE, 20000), GraphManager.SCHEDULE_RATE));
        assertEquals(2, GraphManager.countStagesWithNotaKey(GraphManager.cloneStagesWithNotaKey(gm, GraphManager.SCHEDULE_RATE), GraphManager.SCHEDULE_RATE));
        
        
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 30000, b); //change value
        assertEquals(0, GraphManager.countStagesWithNotaKey(GraphManager.cloneStagesWithNotaKeyValue(gm, GraphManager.SCHEDULE_RATE, 20000), GraphManager.SCHEDULE_RATE));
        
        //force Nota array growth, must be larger than 34
        int j = 34;
        while (--j>=0) {
            GraphManager.addNota(gm, Integer.toHexString(j), j, a);
        }
        
        //add same nota to all 3
        GraphManager.addNota(gm, "ANota", 70000, a, b, c);
        assertEquals(3, GraphManager.countStagesWithNotaKey(gm, "ANota"));
        
    }
	
	@Test
	public void constructionOfSimpleGraph() {
		
		GraphManager gm = new GraphManager();
		
		Pipe rb1 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb1.initBuffers();
		
		Pipe rb2 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb2.initBuffers();
		
		PronghornStage a = new SimpleOut(gm,rb1); 
		PronghornStage b = new SimpleInOut(gm,rb1,rb2); 
		PronghornStage c = new SimpleIn(gm,rb2); 
			
		
		//
		//testing
		//
		
		assertTrue(a == GraphManager.getStage(gm,a.stageId));
		assertTrue(b == GraphManager.getStage(gm,b.stageId));
		assertTrue(c == GraphManager.getStage(gm,c.stageId));
		
		assertTrue(rb1 == GraphManager.getPipe(gm,rb1.id));
		assertTrue(rb2 == GraphManager.getPipe(gm,rb2.id));
		
		assertTrue(a == GraphManager.getRingProducer(gm,rb1.id));
		assertTrue(b == GraphManager.getRingConsumer(gm,rb1.id));
		assertTrue(b == GraphManager.getRingProducer(gm,rb2.id));
		assertTrue(c == GraphManager.getRingConsumer(gm,rb2.id));
		
		Pipe.addMsgIdx(rb1,  0);
		Pipe.addIntValue(101, rb1); //add a single int to the ring buffer
		
		Pipe.publishWrites(rb1);

		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		GraphManager.setStateToStopping(gm, a.stageId);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
				
		Pipe.releaseReadLock(rb1);
		GraphManager.setStateToStopping(gm, a.stageId);
		GraphManager.setStateToStopping(gm, b.stageId);

		assertTrue(GraphManager.mayHaveUpstreamData(gm, c.stageId)); //this is true because the first ring buffer has 1 integer
		
		
	}
	
	@Test
	public void constructionOfForkedGraph() {
		
		GraphManager gm = new GraphManager();
		
		Pipe rb1 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb1.initBuffers();
		Pipe rb21 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb21.initBuffers();
		Pipe rb22 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb22.initBuffers();
		
		Pipe rb211 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb211.initBuffers();
		
		Pipe rb221 = new Pipe(new PipeConfig(RawDataSchema.instance));
		rb221.initBuffers();
		
		PronghornStage a = new SimpleOut(gm,rb1); 
		PronghornStage b = new SimpleInOutSplit(gm, rb1, rb21, rb22); 
		
		PronghornStage b1 = new SimpleInOut(gm, rb21, rb211); 
		PronghornStage b2 = new SimpleInOut(gm, rb22, rb221); 
					
		PronghornStage c1 = new SimpleIn(gm, rb211); 
		PronghornStage c2 = new SimpleIn(gm, rb221);
		
		Pipe.addMsgIdx(rb1,  0);
		Pipe.addIntValue(101, rb1); //add a single int to the ring buffer
		Pipe.publishWrites(rb1);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
		
		GraphManager.setStateToStopping(gm, a.stageId);
				
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer
				
		Pipe.releaseReadLock(rb1);
		
		GraphManager.setStateToStopping(gm, a.stageId);
		GraphManager.setStateToStopping(gm, b.stageId);
		GraphManager.setStateToStopping(gm, b1.stageId);
		GraphManager.setStateToStopping(gm, b2.stageId);
		
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c1.stageId)); //this is true because the first ring buffer has 1 integer
		assertTrue(GraphManager.mayHaveUpstreamData(gm, c2.stageId)); //this is true because the first ring buffer has 1 integer

		
	}
	
	
	private class SimpleInOut extends PronghornStage {

		protected SimpleInOut(GraphManager pm, Pipe input, Pipe output) {
			super(pm, input, output);
		}

		@Override
		public void run() {
		}
		
	}
	
	private class SimpleInOutSplit extends PronghornStage {

		protected SimpleInOutSplit(GraphManager pm, Pipe input, Pipe ... output) {
			super(pm, input, output);
		}

		@Override
		public void run() {
		}
		
	}

	private class SimpleOut extends PronghornStage {

		protected SimpleOut(GraphManager pm, Pipe output) {
			super(pm, NONE, output);
		}

		@Override
		public void run() {
		}
		
	}
	
	private class SimpleIn extends PronghornStage {

		protected SimpleIn(GraphManager pm, Pipe input) {
			super(pm, input , NONE);
		}

		@Override
		public void run() {
		}
		
	}
	
}
