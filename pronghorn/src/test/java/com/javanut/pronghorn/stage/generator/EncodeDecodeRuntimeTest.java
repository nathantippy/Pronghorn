package com.javanut.pronghorn.stage.generator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.javanut.pronghorn.code.LoaderUtil;
import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchemaDynamic;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeConfig;
import com.javanut.pronghorn.pipe.RawDataSchema;
import com.javanut.pronghorn.pipe.schema.loader.TemplateHandler;
import com.javanut.pronghorn.stage.IntegrityFuzzGenerator;
import com.javanut.pronghorn.stage.IntegrityTestFuzzConsumer;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.javanut.pronghorn.stage.generator.PhastEncoderStageGenerator;
import com.javanut.pronghorn.stage.scheduling.GraphManager;
import com.javanut.pronghorn.stage.scheduling.ThreadPerStageScheduler;

/**
 * Created by jake on 10/29/16.
 */
public class EncodeDecodeRuntimeTest {
    @Test
    public void compileTest() throws Exception {
        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/template/integrityTest.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);

        StringBuilder target = new StringBuilder();
        PhastDecoderStageGenerator ew = new PhastDecoderStageGenerator(messageSchema, target, false);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }


        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target, PhastDecoderStageGenerator.class);

        StringBuilder target2 = new StringBuilder();
        PhastEncoderStageGenerator encoder = new PhastEncoderStageGenerator(messageSchema, target2);

        try {
            encoder.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }


        validateCleanCompile(encoder.getPackageName(), encoder.getClassName(), target2, PhastEncoderStageGenerator.class);

    }

    @Ignore
    public void runTimeTest() throws Exception {
        GraphManager gm = new GraphManager();

        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/template/integrityTest.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);
        Pipe<MessageSchemaDynamic> inPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        inPipe.initBuffers();
        Pipe<RawDataSchema> sharedPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));
        sharedPipe.initBuffers();
        Pipe<MessageSchemaDynamic> outPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        outPipe.initBuffers();
        //get encoder ready
        StringBuilder eTarget = new StringBuilder();
        PhastEncoderStageGenerator ew = new PhastEncoderStageGenerator(messageSchema, eTarget);
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        Constructor econstructor =  LoaderUtil.generateThreeArgConstructor(ew.getPackageName(), ew.getClassName(), eTarget, PhastEncoderStageGenerator.class);

        //get decoder ready
        StringBuilder dTarget = new StringBuilder();
        PhastDecoderStageGenerator dw = new PhastDecoderStageGenerator(messageSchema, dTarget, false);
        try {
            dw.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        Constructor dconstructor =  LoaderUtil.generateThreeArgConstructor(dw.getPackageName(), dw.getClassName(), dTarget, PhastDecoderStageGenerator.class);

        IntegrityFuzzGenerator random1 = new IntegrityFuzzGenerator(gm, inPipe);
        econstructor.newInstance(gm, inPipe, sharedPipe);
        dconstructor.newInstance(gm, sharedPipe, outPipe);
        StringBuilder result = new StringBuilder();
        IntegrityTestFuzzConsumer consumer = new IntegrityTestFuzzConsumer(gm, outPipe, result);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();


        //GraphManager.blockUntilStageBeginsShutdown(gm,json);
        scheduler.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue("44633,2,42363,41696,15806,2,35054,46050".equals(result.toString()));

    }

    private static void validateCleanCompile(String packageName, String className, StringBuilder target, Class clazz) {
        try {

            Class generateClass = LoaderUtil.generateClass(packageName, className, target, clazz);
            if (null==generateClass) {
            	assertTrue(true);//we have no compiler
            	return;
            }
            if (generateClass.isAssignableFrom(PronghornStage.class)) {
                Constructor constructor =  generateClass.getConstructor(GraphManager.class, Pipe.class, Pipe.class);
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
}
