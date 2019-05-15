package com.javanut.pronghorn.stage.blocking;

public interface BlockingWorkerProducer  {

	BlockingWorker newWorker();

	String name();
	
	
}
