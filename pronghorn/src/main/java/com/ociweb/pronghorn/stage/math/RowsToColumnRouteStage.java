package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 *
 * @param <M>
 */
public class RowsToColumnRouteStage<M extends MatrixSchema<M>> extends PronghornStage {

	private final Pipe<RowSchema<M>> rowPipeInput; 
	private final Pipe<ColumnSchema<M>>[] columnPipeOutput;
	private final M matrixSchema;
	private final int rowLimit;
	private final int sizeOf;
	private int remainingRows;
	private boolean shutdownInProgress;
	
	protected RowsToColumnRouteStage(GraphManager graphManager, Pipe<RowSchema<M>> rowPipeInput, Pipe<ColumnSchema<M>>[] columnPipeOutput) {
		super(graphManager, rowPipeInput, columnPipeOutput);
		
		M matrixSchema = (M) rowPipeInput.config().schema().rootSchema();
		this.rowPipeInput = rowPipeInput;
		this.columnPipeOutput = columnPipeOutput;
		this.matrixSchema = matrixSchema;
		this.rowLimit = matrixSchema.getRows();
		this.remainingRows = rowLimit;
		this.sizeOf = Pipe.sizeOf(columnPipeOutput[0], matrixSchema.columnId);
		if (matrixSchema.columns != columnPipeOutput.length) {
			throw new UnsupportedOperationException(matrixSchema.columns+" was expected to match "+columnPipeOutput.length);
		}
	}

	
	@Override
	public void shutdown() {
		Pipe.publishEOF(columnPipeOutput);
	}
	
	@Override
	public void run() {
		
		 if(shutdownInProgress) {
	    	 int i = columnPipeOutput.length;
	         while (--i >= 0) {
	         	if (null!=columnPipeOutput[i] && Pipe.isInit(columnPipeOutput[i])) {
	         		if (!Pipe.hasRoomForWrite(columnPipeOutput[i], Pipe.EOF_SIZE)){ 
	         			return;
	         		}  
	         	}
	         }
	         requestShutdown();
	         return;
    	 }
		
		
		int columnIdx = 0;
		final int columnLimit = columnPipeOutput.length;
		final int typeSize = matrixSchema.typeSize;

		
		//TODO: by waiting for all outputs to have room then 1 of the compute units can stop any of the others.
	    //      can we write to those that are available while we wait?			
		
		while (Pipe.hasContentToRead(rowPipeInput) && ((remainingRows<rowLimit) || allHaveRoomToWrite(columnPipeOutput))  ) {
						
			    ///////////////////////////
				//begin processing this row
			    ///////////////////////////
			
				int rowId = Pipe.takeMsgIdx(rowPipeInput);
				if (rowId<0) {
					assert(remainingRows==rowLimit);
					Pipe.confirmLowLevelRead(rowPipeInput, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(rowPipeInput);
					shutdownInProgress = true;
					return;
				}
				
				//////////////////////
				//If we are on the first row then open the output column messages
				//////////////////////
				if (remainingRows==rowLimit) {
					int c = columnLimit;
					while (--c>=0) {
						Pipe.addMsgIdx(columnPipeOutput[c], matrixSchema.columnId);
					}				 
				}
				remainingRows--;
				
				//////////////////////////////////
				//splits row oriented matrix input into columns
				/////
				//    [1  2  3
				//     4  5  6]  
				////
				//becomes
				///
				//    [1  4]
				//    [2  5]
				//    [3  6]
						
				long sourceLoc = Pipe.getWorkingTailPosition(rowPipeInput);				
				for(columnIdx=0;columnIdx<columnLimit;columnIdx++) {
						
					long targetLoc = Pipe.workingHeadPosition(columnPipeOutput[columnIdx]);
					
					Pipe.copyIntsFromToRing(Pipe.slab(rowPipeInput), (int)sourceLoc, Pipe.slabMask(rowPipeInput), 
							                Pipe.slab(columnPipeOutput[columnIdx]), (int)targetLoc, Pipe.slabMask(columnPipeOutput[columnIdx]), typeSize);

					sourceLoc += (long)typeSize;
					Pipe.setWorkingHead(columnPipeOutput[columnIdx], targetLoc+(long)typeSize);
											
				}				
				Pipe.setWorkingTailPosition(rowPipeInput, sourceLoc);
				
				
				//always confirm writes before reads.
				if (remainingRows==0) {
								
					int c = columnLimit;
					while (--c>=0) {
						
						//System.out.println("wrote col "+sizeOf);
						Pipe.confirmLowLevelWrite(columnPipeOutput[c], sizeOf);					
						Pipe.publishWrites(columnPipeOutput[c]);					
					}
					remainingRows = rowLimit;
				}
				
				//release this row we are done processing it against all the columns
				Pipe.confirmLowLevelRead(rowPipeInput, Pipe.sizeOf(rowPipeInput, rowId));
				Pipe.releaseReadLock(rowPipeInput);
			
		}
		
		
	}

	private boolean allHaveRoomToWrite(Pipe<ColumnSchema<M>>[] columnPipeOutput) {
		int i = columnPipeOutput.length;
		while (--i>=0) {
			if (!Pipe.hasRoomForWrite(columnPipeOutput[i])) {
				return false;
			}
		}
		return true;
	}

}
