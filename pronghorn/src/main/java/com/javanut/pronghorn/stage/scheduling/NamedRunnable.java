package com.javanut.pronghorn.stage.scheduling;

public interface NamedRunnable extends Runnable {

	String name();

	void setThreadId(long id);
	
}
