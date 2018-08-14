package com.ociweb.pronghorn.pipe.stream;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

public class LowLevelStateManager {
    final int[] cursorStack;
    private final int[] sequenceCounters;
    int nestedFragmentDepth;
    private final int[] fragScriptSize;

    public LowLevelStateManager(FieldReferenceOffsetManager from) {
        this.cursorStack = null==from?null:new int[from.maximumFragmentStackDepth];
        this.sequenceCounters = null==from?null:new int[from.maximumFragmentStackDepth];        
        this.fragScriptSize = null==from?null:from.fragScriptSize;
        
        //publish only happens on fragment boundary therefore we can assume that if 
        //we can read 1 then we can read the full fragment
        
        this.nestedFragmentDepth = -1; 
    }

    public static int processGroupLength(LowLevelStateManager that, final int cursor, int seqLen) {
        int fDepth = 1+that.nestedFragmentDepth;
        that.sequenceCounters[fDepth]= seqLen;
        that.cursorStack[fDepth] = cursor+that.fragScriptSize[cursor];
        that.nestedFragmentDepth = fDepth;
        return seqLen;
    }

    public static int activeCursor(LowLevelStateManager that) {
        return that.cursorStack[that.nestedFragmentDepth];
    }

    public static boolean isStartNewMessage(LowLevelStateManager that) {
        return that.nestedFragmentDepth<0;
    }

    public static int closeFragment(LowLevelStateManager that) {
        return that.nestedFragmentDepth--;
    }

    public static int interationIndex(LowLevelStateManager that) {
        return Math.max(0, that.sequenceCounters[that.nestedFragmentDepth]-1);
    }
    
    public static boolean closeSequenceIteration(LowLevelStateManager that) {
        return --that.sequenceCounters[that.nestedFragmentDepth]<=0;
    }

    public static void continueAtThisCursor(LowLevelStateManager that, int fieldCursor) {
        that.cursorStack[++that.nestedFragmentDepth] = fieldCursor;
    }
}