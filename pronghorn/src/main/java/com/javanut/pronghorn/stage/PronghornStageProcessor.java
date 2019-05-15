package com.javanut.pronghorn.stage;

import com.javanut.pronghorn.stage.scheduling.GraphManager;

public interface PronghornStageProcessor {

	public void process(GraphManager gm, PronghornStage stage);
}
