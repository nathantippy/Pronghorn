package com.javanut.pronghorn.network.http;

public interface ErrorReporter {

	boolean sendError(long id, int errorCode);

}
