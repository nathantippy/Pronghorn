package com.javanut.pronghorn.stage.file;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import com.javanut.pronghorn.pipe.util.build.FROMValidation;
import com.javanut.pronghorn.stage.file.schema.BlockManagerRequestSchema;
import com.javanut.pronghorn.stage.file.schema.BlockManagerResponseSchema;
import com.javanut.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.javanut.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.javanut.pronghorn.stage.file.schema.FolderWatchSchema;
import com.javanut.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.javanut.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.javanut.pronghorn.stage.file.schema.PersistedBlobLoadReleaseSchema;
import com.javanut.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.javanut.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
import com.javanut.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.javanut.pronghorn.stage.file.schema.SequentialRespSchema;

public class SchemaTest {

	private static final String ROOT = "src" + File.separator + "test" + File.separator + "resources" + File.separator;

	@Test
	public void testBlockManagerRequestSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockManagerRequest.xml", BlockManagerRequestSchema.class));
	}

	@Test
	public void testBlockManagerResponseSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockManagerResponse.xml", BlockManagerResponseSchema.class));
	}

	@Test
	public void testPersistedBlobLoadConsumerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobLoadConsumer.xml", PersistedBlobLoadConsumerSchema.class));
	}

	@Test
	public void testPersistedBlobLoadReleaseSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobLoadRelease.xml", PersistedBlobLoadReleaseSchema.class));
	}
	
	@Test
	public void testPersistedBlobLoadProducerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobLoadProducer.xml", PersistedBlobLoadProducerSchema.class));
	}

	@Test
	public void testPersistedBlobSaveConsumerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobStoreConsumer.xml", PersistedBlobStoreConsumerSchema.class));

	}
	
	@Test
	public void testPersistedBlobSaveProducerSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "PersistedBlobStoreProducer.xml", PersistedBlobStoreProducerSchema.class));

	}
	
	@Test
	public void testSequentialFileControlSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "SequentialCtl.xml", SequentialCtlSchema.class));
	}
	
	@Test
	public void testSequentialFileResponseSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "SequentialResp.xml", SequentialRespSchema.class));
	}
	
	@Test
	public void testBlockStorageXmitSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockStorageXmit.xml", BlockStorageXmitSchema.class));
	}
	
	@Test
	public void testBlockStorageReceiveSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "BlockStorageReceive.xml", BlockStorageReceiveSchema.class));
	}
	
	@Test
	public void testFolderWatchSchema() {
		assertTrue(FROMValidation.checkSchema(ROOT + "FolderWatch.xml", FolderWatchSchema.class));
	}

}
