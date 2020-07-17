package com.javanut.pronghorn.stage.generator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.junit.Test;

import com.javanut.pronghorn.code.LoaderUtil;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.generator.FuzzDataStageGenerator;
import com.javanut.pronghorn.stage.generator.PhastEncoderStageGenerator;
import com.javanut.pronghorn.stage.monitor.PipeMonitorSchema;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class PhastEncoderStageGeneratorTest {

    @Test
    public void generateEncoderCompileTest() {
        
        StringBuilder target = new StringBuilder();
        PhastEncoderStageGenerator ew = new PhastEncoderStageGenerator(PipeMonitorSchema.instance, target);

        try {
            ew.processSchema();
        } catch (IOException e) {
           // System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
        
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

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
           // System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (NoSuchMethodException e) {
           // System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
           // System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
    }

    //TODO: Add speed tests here

    
}
