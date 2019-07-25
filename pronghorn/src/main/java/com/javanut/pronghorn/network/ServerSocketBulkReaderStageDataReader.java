package com.javanut.pronghorn.network;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.Consumer;

public class ServerSocketBulkReaderStageDataReader {
	
	public final ServerCoordinator coordinator;
	public Selector selector;
	public Set<SelectionKey> selectedKeys;
	public ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);

    public boolean hasRoomForMore = true;
    public final Consumer<SelectionKey> selectionKeyAction;
    
    
	public ServerSocketBulkReaderStageDataReader(ServerCoordinator coordinator, Consumer<SelectionKey> selectionKeyAction) {
		this.coordinator = coordinator;	
		this.selectionKeyAction = selectionKeyAction;
		
	}

	public void registerSelector() {
        try {//must be done early to ensure this is ready before the other stages startup.
        	coordinator.registerSelector(selector = Selector.open());
        } catch (IOException e) {
        	throw new RuntimeException(e);
        }
	}

	public void generateSocketHolder() {
		  ServerCoordinator.newSocketChannelHolder(coordinator);
		  
	}
	
	
	
}