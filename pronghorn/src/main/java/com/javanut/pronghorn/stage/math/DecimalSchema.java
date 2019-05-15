package com.javanut.pronghorn.stage.math;

import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;

public class DecimalSchema<M extends MatrixSchema<M>> extends MatrixSchema<DecimalSchema<M>> {

	public DecimalSchema(MatrixSchema<M> matrixSchema) {
		super(matrixSchema.getColumns(), 1, MatrixTypes.Decimals, FieldReferenceOffsetManager.buildSingleNumberBlockFrom(matrixSchema.columns, MatrixTypes.Decimals.typeMask, "Matrix"));
	}

}
