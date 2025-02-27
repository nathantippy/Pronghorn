package com.javanut.pronghorn.stage.test;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.stage.route.ReplicatorStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

public class JSONTap {

	public static <T extends MessageSchema<T>> Pipe<T> attach(boolean debug, GraphManager gm, Pipe<T> source, Appendable console) {
		if (debug) {
			Pipe<T> out1 = new Pipe<T>(source.config().grow2x());
			Pipe<T> out2 = new Pipe<T>(source.config().grow2x());
					
			ReplicatorStage.newInstance(gm, source, out1, out2);		
			ConsoleJSONDumpStage.newInstance(gm, out1, console);
	
			return out2; 
		} else {
			return source;
		}
	}

}
