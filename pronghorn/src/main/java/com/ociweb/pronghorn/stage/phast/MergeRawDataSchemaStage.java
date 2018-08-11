package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * Merges multiple RawDataSchema into one
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class MergeRawDataSchemaStage extends PronghornStage {

    //TODO: modify to take any simple schema, nothing with nested messages.
    
    private final Pipe<RawDataSchema>[] inputs;
    private final Pipe<RawDataSchema> output;
    private int pipeIdx = 0;
    
    private final static int OUTPUT_MAX_MSG_SIZE = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
    
    private PaddedLong outputHead;
    private PaddedLong[] inputTail;
    
    long outputCount;
    long[] inputCount;

    /**
     *
     * @param graphManager
     * @param inputs _in_ Multiple RawDataSchema that shoud be merged into one
     * @param output _out_ Resulting single RawDataSchema from multiple RawDataSchema
     */
    protected MergeRawDataSchemaStage(GraphManager graphManager, Pipe<RawDataSchema>[] inputs, Pipe<RawDataSchema> output) {
        super(graphManager, inputs, output);
        this.inputs = inputs;
        this.output = output;
        
        supportsBatchedPublish = false;
        supportsBatchedRelease  = false;
        
    }
    
    @Override
    public void startup() {
        this.outputHead = Pipe.getWorkingHeadPositionObject(output);
        
        int i = inputs.length;
        this.inputCount = new long[i];
        this.inputTail = new PaddedLong[i];
        while (--i >= 0) {
            inputTail[i] = Pipe.getWorkingTailPositionObject(inputs[i]);
        }
        
    }
    
    @Override
    public void run() {
        
        //TODO: refactor back to normal low level calls and check the performance changes.
        
        int[] outputSlab = Pipe.slab(output);
        int outputMask = Pipe.slabMask(output);
        PaddedLong localHead = outputHead;
        Pipe<RawDataSchema> localOutput = output;
        
        if (outputCount <= 0) {
            outputCount = localOutput.sizeOfSlabRing-(localHead.value-Pipe.tailPosition(localOutput));
        }
        
        int inputsCount = inputs.length;
        Pipe<RawDataSchema> activeInput = inputs[pipeIdx];
        PaddedLong localTail = inputTail[pipeIdx];
        int localPipeIdx = pipeIdx;
        
        if (inputCount[localPipeIdx] < OUTPUT_MAX_MSG_SIZE) {
            inputCount[localPipeIdx] = activeInput.sizeOfSlabRing-(localTail.value-Pipe.headPosition(activeInput));
        }
                
        while ((outputCount >= OUTPUT_MAX_MSG_SIZE) && (inputCount[localPipeIdx] >= OUTPUT_MAX_MSG_SIZE) ) {
                               
                outputCount -= OUTPUT_MAX_MSG_SIZE;            
                inputCount[localPipeIdx] -= OUTPUT_MAX_MSG_SIZE;
                    
                int inputMask = Pipe.slabMask(activeInput); 
                int[] inputSlab = Pipe.slab(activeInput);
                int inputMsgIdx = inputSlab[inputMask & (int) localTail.value++];    
                
                if (inputMsgIdx<0) {
                    requestShutdown();
                    return;
                }
                            
                Pipe.markBytesWriteBase(localOutput);            
                outputSlab[outputMask & (int) localHead.value++] = RawDataSchema.MSG_CHUNKEDSTREAM_1;      
                
                int inputMeta = Pipe.takeByteArrayMetaData(activeInput);
                int inputLength    = Pipe.takeByteArrayLength(activeInput);
                Pipe.addByteArrayWithMask(localOutput, Pipe.blobMask(activeInput), inputLength, Pipe.blob(activeInput), Pipe.bytePosition(inputMeta, activeInput, inputLength));                  
                                
                outputSlab[outputMask & (int) localHead.value++] = inputLength;
                
                Pipe.publishHeadPositions(localOutput);
                Pipe.markBytesReadBase(activeInput, inputSlab[inputMask & (int) localTail.value++]);
                Pipe.batchedReleasePublish(activeInput, Pipe.getWorkingBlobTailPosition(activeInput), localTail.value);
                
                if (++localPipeIdx >= inputsCount) {
                    localPipeIdx = 0;
                }
                activeInput = inputs[localPipeIdx];
                localTail = inputTail[localPipeIdx];
                
                if (inputCount[localPipeIdx] < OUTPUT_MAX_MSG_SIZE) {
                    inputCount[localPipeIdx] = activeInput.sizeOfSlabRing-(localTail.value-Pipe.headPosition(activeInput));
                }
                if (outputCount < OUTPUT_MAX_MSG_SIZE) {
                    outputCount = localOutput.sizeOfSlabRing-(localHead.value-Pipe.tailPosition(localOutput));
                }
        }
        pipeIdx = localPipeIdx;
        
    }

    
}
