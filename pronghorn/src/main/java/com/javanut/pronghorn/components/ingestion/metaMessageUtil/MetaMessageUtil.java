package com.javanut.pronghorn.components.ingestion.metaMessageUtil;

import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.schema.loader.TemplateHandler;


public class MetaMessageUtil  {

    public static FieldReferenceOffsetManager buildFROM(String source) {
    	try {
			return TemplateHandler.loadFrom(source);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }

}
