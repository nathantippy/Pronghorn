package com.javanut.pronghorn.network.config;

public interface HTTPContentType {

    CharSequence contentType();
    
    CharSequence fileExtension();
    byte[] getBytes();
    
    int ordinal();
    
    boolean isAlias();
    
    
}
