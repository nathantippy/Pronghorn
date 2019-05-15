package com.javanut.pronghorn.pipe;

public class SchemalessFixedFieldPipeConfig extends PipeConfig<MessageSchemaDynamic> {

	public SchemalessFixedFieldPipeConfig(int slabSize) {
		super(slabSize, new MessageSchemaDynamic(null));
	}
}
