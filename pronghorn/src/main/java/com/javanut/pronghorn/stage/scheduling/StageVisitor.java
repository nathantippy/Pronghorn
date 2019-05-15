package com.javanut.pronghorn.stage.scheduling;

import com.javanut.pronghorn.stage.PronghornStage;

public interface StageVisitor {

	void visit(PronghornStage stage);

}
