package com.javanut.json;

import com.javanut.pronghorn.struct.StructRegistry;
import com.javanut.pronghorn.util.TrieParser;
import com.javanut.pronghorn.util.parse.JSONStreamVisitorToChannel;

public interface JSONExtractorCompleted {
	TrieParser trieParser();
	JSONStreamVisitorToChannel newJSONVisitor();
	void addToStruct(StructRegistry schema, int structId);
	
	int[] getIndexPositions();
	int getStructId();
	void debugSchema();
	
}

