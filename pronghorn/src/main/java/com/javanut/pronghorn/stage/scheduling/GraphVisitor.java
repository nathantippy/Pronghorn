package com.javanut.pronghorn.stage.scheduling;

public interface GraphVisitor {

	boolean visit(GraphManager graphManager, int stageId, int depth);

}
