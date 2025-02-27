package com.javanut.pronghorn.stage.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.javanut.pronghorn.code.LoaderUtil;
import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.MessageSchemaDynamic;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeConfig;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.pipe.schema.loader.TemplateHandler;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.generator.FuzzDataStageGenerator;
import com.javanut.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.javanut.pronghorn.stage.monitor.PipeMonitorSchema;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.stage.scheduling.NonThreadScheduler;
import com.javanut.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.javanut.pronghorn.stage.test.ConsoleSummaryStage;

public class FuzzGeneratorGeneratorTest {

    
    @Test
    public void fuzzGeneratorBuildTest() {
        
        StringBuilder target = new StringBuilder();
        FuzzDataStageGenerator ew = new FuzzDataStageGenerator(PipeMonitorSchema.instance, target);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    @Test
    public void fuzzGeneratorBuildRunnableTest() {

        StringBuilder target = new StringBuilder();
        FuzzDataStageGenerator ew = new FuzzDataStageGenerator(PipeMonitorSchema.instance, target, true);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }        
        
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    
    @Test
    public void fuzzGeneratorBuildRunnable2Test() {
        MessageSchemaDynamic schema = sequenceExampleSchema();               
        
        StringBuilder target = new StringBuilder();
        FuzzDataStageGenerator ew = new FuzzDataStageGenerator(schema, target, true);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
       // System.out.println(target);
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    @Test
    public void fuzzGeneratorUsageTestPipeMonitor() {
        
        StringBuilder target = new StringBuilder();
        
        FuzzDataStageGenerator ew = new FuzzDataStageGenerator(PipeMonitorSchema.instance, target);
        // //set the field to use for latency
        ew.setTimeFieldId(1); //use the MS field from the monitor schema to put the time into.
        
        int durationMS = 200;
        
        runtimeTestingOfFuzzGenerator(target, PipeMonitorSchema.instance, ew, durationMS, 200);
    }
    
    @Test
    public void fuzzGeneratorUsageTestSequenceExample() {

            MessageSchemaDynamic schema = sequenceExampleSchema();
        
            
            //Fuzz time test,  what ranges are the best?
            
            
            StringBuilder target = new StringBuilder();
            
            FuzzDataStageGenerator ew = new FuzzDataStageGenerator(schema, target);
            ew.setMaxSequenceLengthInBits(9);
            
            int durationMS = 100; 
            
            runtimeTestingOfFuzzGenerator(target, schema, ew, durationMS, 8000);

        
    }

    private MessageSchemaDynamic sequenceExampleSchema() {
        try {
            MessageSchemaDynamic schema;
            schema = new MessageSchemaDynamic(TemplateHandler.loadFrom("/template/sequenceExample.xml"));
            return schema;
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            fail();
        } catch (SAXException e) {
            e.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        return null;
    }
    
    @Ignore
    public void fuzzGeneratorUsageTestRawData() {
        
        StringBuilder target = new StringBuilder();
                
        //TODO: Rewrite for ByteVector, DO NOT use ByteBuffer instead use the easier DataOutput object.
        FuzzDataStageGenerator ew = new FuzzDataStageGenerator(RawDataSchema.instance, target);
        
        
        int durationMS = 300;
        
        runtimeTestingOfFuzzGenerator(target, RawDataSchema.instance, ew, durationMS,1000);
    }

    private void runtimeTestingOfFuzzGenerator(StringBuilder target, MessageSchema schema, FuzzDataStageGenerator ew, int durationMS, int pipeLength) {
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        
        
             
        
        try {
            Constructor constructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), target, FuzzDataStageGenerator.class);
            
            if (null==constructor) {
            	return; //do not test, we have no compiler.
            }
            
            GraphManager gm = new GraphManager();
            
            //NOTE: Since the ConsoleSummaryStage usess the HighLevel API the pipe MUST be large enough to hold and entire message
            //      Would be nice to detect this failure, not sure how.
            Pipe<?> pipe = new Pipe<>(new PipeConfig<>(schema, pipeLength));           
            
            constructor.newInstance(gm, pipe);
            Appendable out = new PrintWriter(new ByteArrayOutputStream());
            ConsoleSummaryStage dump = new ConsoleSummaryStage(gm, pipe, out );
            
            GraphManager.enableBatching(gm);

            NonThreadScheduler scheduler = new NonThreadScheduler(gm);
            scheduler.startup();
            
            long limit = System.currentTimeMillis()+durationMS;
            while (System.currentTimeMillis()<limit) {
            	scheduler.run();
            }
            scheduler.shutdown();
            
            
            
            
        } catch (Exception e) {
            e.printStackTrace();
        } 
    }
     
    
    
    private static void validateCleanCompile(String packageName, String className, StringBuilder target) {
        try {

            Class generateClass = LoaderUtil.generateClass(packageName, className, target, FuzzDataStageGenerator.class);
            if (null==generateClass) {
            	assertTrue(true);//we have no compiler
            	return;
            }
            
            if (generateClass.isAssignableFrom(PronghornStage.class)) {
                Constructor constructor =  generateClass.getConstructor(GraphManager.class, Pipe.class);
                assertNotNull(constructor);
            }
        
        } catch (ClassNotFoundException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (NoSuchMethodException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
    }
    
    //TODO: methods below this point need to be moved to static helper class.

    
    
}
