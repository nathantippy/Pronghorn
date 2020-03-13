package com.javanut.pronghorn.pipe;

public interface Writable {

	Writable NO_OP = new Writable() {
		@Override
		public void write(ChannelWriter writer) {
		}		
	};
	
	void write(ChannelWriter writer); //returns true if we have more data to write.

}
