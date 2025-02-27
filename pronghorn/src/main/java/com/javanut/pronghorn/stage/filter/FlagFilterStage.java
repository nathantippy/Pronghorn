package com.javanut.pronghorn.stage.filter;

import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.pipe.PipeWriter;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * Filter messages based on flags.
 * @param <T>
 * @author Nathan Tippy
 * @see <a href="https://github.com/nathantippy/Pronghorn">Pronghorn</a>
 */
public class FlagFilterStage<T extends MessageSchema<T>> extends PronghornStage {

    //after instances of each id then let them pass
    //saves its own state
    
    private final Pipe<T> input;
    private final Pipe<T> output;
    private final int varFieldLoc;
    private final int mask;
    private int count;
    boolean moveInProgress = false;

    /**
     *
     * @param graphManager
     * @param input _in_ Input message that will be filtered.
     * @param output _out_ Filtered messages appear on this pipe.
     * @param varFieldLoc
     * @param mask
     */
    public FlagFilterStage(GraphManager graphManager, Pipe<T> input, Pipe<T> output, int varFieldLoc, int mask) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.varFieldLoc = varFieldLoc;
        this.mask = mask;
    }

    @Override
    public void startup() {
    }

    
    @Override
    public void shutdown() {
    }
    
    
    
    @Override
    public void run() {
        
        if (moveInProgress) {
            if (!PipeReader.tryMoveSingleMessage(input, output)) {
                return;
            } else {
                moveInProgress = false;
                PipeReader.releaseReadLock(input);
            }
        }
        
        while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
                        
            count++;
            int value = PipeReader.readInt(input, varFieldLoc);  
            
            if (0 != (mask&value)) {
                if (!PipeReader.tryMoveSingleMessage(input, output)) {
                    moveInProgress = true;
                    return;
                }              
            }
            PipeReader.releaseReadLock(input);
        }        
    }    

}
