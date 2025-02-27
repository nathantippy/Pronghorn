package com.javanut.pronghorn.stage;

import com.javanut.pronghorn.code.StageTester;
import com.javanut.pronghorn.network.ClientCoordinator;
import com.javanut.pronghorn.network.ClientSocketReaderStage;
import com.javanut.pronghorn.network.TLSCertificates;
import com.javanut.pronghorn.network.TLSCerts;
import com.javanut.pronghorn.struct.StructRegistry;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FuzzTests {

	private final static int testDuration = 100;
	private int generatorSeed = 10;
	
	@Test
	public void fuzzClientSocketReaderStage() {
		TLSCertificates certs = TLSCerts.define();
		assertTrue(StageTester.runFuzzTest(ClientSocketReaderStage.class, testDuration, generatorSeed++,
				new Object[]{new ClientCoordinator(4, 10, certs, new StructRegistry()),false}) );
	}

//	@Test
//	public void fuzzClientSocketWriterStage() {
//		assertTrue(StageTester.runFuzzTest(ClientSocketWriterStage.class, testDuration, generatorSeed++,
//				new Object[]{new ClientCoordinator(3, 10),false}) );
//	}
	
//	@Test
//	public void fuzzNetResponseDumpStage() {
//		assertTrue(StageTester.runFuzzTest(NetResponseDumpStage.class, testDuration, generatorSeed++,
//				new Object[]{new StringBuilder() }) );
//	}

	
}
