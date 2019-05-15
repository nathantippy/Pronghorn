package com.javanut.pronghorn.pipe;

import com.javanut.pronghorn.pipe.token.TypeMask;

public class SeriesDecimalSchema extends MessageSchema<SeriesDecimalSchema> {
	
	protected SeriesDecimalSchema() {
		super(FieldReferenceOffsetManager.buildAggregateNumberBlockFrom(TypeMask.Decimal, "Series"));
	}

	public static final SeriesDecimalSchema instance = new SeriesDecimalSchema();

}
