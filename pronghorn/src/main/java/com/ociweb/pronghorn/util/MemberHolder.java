package com.ociweb.pronghorn.util;

import java.nio.Buffer;
import java.nio.ByteBuffer;

public class MemberHolder {

    private final ByteBuffer[] data; //pos remains at end ready for write.
    private static final int SIZE_OF_MAX_PACKED_LONG = 10;
    private static final int DEFAULT_MAX_MEMBERS = 64;
    
    public MemberHolder(int maxSets) {
        this(maxSets, DEFAULT_MAX_MEMBERS * SIZE_OF_MAX_PACKED_LONG);
    }
    
    public MemberHolder(int maxLists, int initBytesPerSet) {
        data = new ByteBuffer[maxLists];
        int i = maxLists;
        while (--i>=0) {
            data[i] = allocate(initBytesPerSet);
        }
    }

    private ByteBuffer allocate(int initBytesPerSet) {
        return ByteBuffer.allocate(initBytesPerSet);
    }
    
    //does not check for duplicates
    //is not thread safe so only one thread should add members
    //TODO: in the future, add Bloom filter to make this function as a set.
    public void addMember(int listId, long member) {   
        
        if (data[listId].remaining()<=10) {
            ///must grow 
            ByteBuffer buff = data[listId];
            ByteBuffer newBuff = allocate(buff.capacity()*2);
            ((Buffer)buff).flip();
            newBuff.put(buff);
            data[listId] = buff = newBuff;
            assert(data[listId].remaining()>10) : "failure in growing ByteBuffer";

        }        
        VarLenLong.writeLongSigned(member, data[listId]);
    }
    
    //removes first match encountered
    //is not thread safe so only one thread should remove members
    public boolean removeMember(int listId, long member) {
        ByteBuffer buff = data[listId];
        ((Buffer)buff).flip();
        while (buff.hasRemaining()) {
            int lastPosition = buff.position();            
            if (member == VarLenLong.readLongSigned(buff)) {
                int newPosition = buff.position();
                int removedCount = newPosition-lastPosition;
                try {
                    //if we can get the backing array there is a fast intrinsic operation to do this.
                    byte[] array = buff.array();
                    System.arraycopy(array, newPosition, array, lastPosition, buff.limit()-lastPosition);    
                    
                    buff.position(buff.limit()-removedCount);
                    buff.limit(buff.capacity());
                    
                } catch (Throwable t) {
                    //must do it the slow way because we can not get the backing array.
                    int oldLimit = buff.limit();
                    
                    ByteBuffer newBuff = allocate(buff.capacity());
                    ((Buffer)buff).position(0);
                    ((Buffer)buff).limit(lastPosition);
                    newBuff.put(buff);
                    
                    ((Buffer)buff).limit(oldLimit);
                    ((Buffer)buff).position(newPosition);
                    newBuff.put(buff);
                    
                    buff = newBuff;  
                    
                }              
                return true;
            }            
        }
        return false;
    }
    
    public boolean isEmpty(int listId) {
        return listId>=data.length ||  0==data[listId].position();
    }
    
    public int containsCount(int listId, long member) {
        if (listId>=data.length) {
            return 0;
        }
        ByteBuffer buff = data[listId];
        ((Buffer)buff).flip();
        int found = 0;
        while (buff.hasRemaining()) {
            if (member == VarLenLong.readLongSigned(buff)) {
                found++;
            }
        }
        ((Buffer)buff).limit(buff.capacity());
        return found;
    }
    
    public void visit(int listId, MemberHolderVisitor visitor) {

        ByteBuffer buff = data[listId];
        ((Buffer)buff).flip();
        while (buff.hasRemaining()) {
            visitor.visit(VarLenLong.readLongSigned(buff));
        }
        visitor.finished();
        ((Buffer)buff).limit(buff.capacity());
    }    
    
}
