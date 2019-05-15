package com.javanut.pronghorn.network.http;

import com.javanut.pronghorn.network.config.HTTPContentType;
import com.javanut.pronghorn.network.config.HTTPHeader;
import com.javanut.pronghorn.network.config.HTTPRevision;
import com.javanut.pronghorn.network.config.HTTPSpecification;
import com.javanut.pronghorn.network.config.HTTPVerb;
import com.javanut.pronghorn.pipe.ChannelReader;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.util.Appendables;


public class HeaderWriter {

	private ChannelWriter activeTarget;
	
	public HeaderWriter(){		
	}
	
	public HeaderWriter target(ChannelWriter activeTarget) {
		this.activeTarget = activeTarget;
		return this;
	}

	/**
	 *
	 * @param header CharSequence to append to activeTarget
	 * @param value CharSequence to append to activeTarget
	 */
	public void write(CharSequence header, CharSequence value) {

			activeTarget.append(header);
			activeTarget.writeByte(':');
			activeTarget.writeByte(' ');			
			activeTarget.append(value);
			activeTarget.writeByte('\r');
			activeTarget.writeByte('\n');
	}
	
	public void write(CharSequence header, long value) {

		activeTarget.append(header);
		activeTarget.writeByte(':');
		activeTarget.writeByte(' ');
		Appendables.appendValue(activeTarget, value);
		activeTarget.writeByte('\r');
		activeTarget.writeByte('\n');
	}
	
	public void writeUTF8(CharSequence header, byte[] value) {
		
			activeTarget.append(header);
			activeTarget.writeByte(':');
			activeTarget.writeByte(' ');
			activeTarget.write(value);
			activeTarget.writeByte('\r');
			activeTarget.writeByte('\n');
	}

	/**
	 *
	 * @param header HTTPHeader to append to activeTarget
	 * @param value CharSequence to append to activeTarget
	 */
	public void write(HTTPHeader header, CharSequence value) {		

		    activeTarget.write(header.rootBytes());
			activeTarget.append(value);
			activeTarget.writeByte('\r');
			activeTarget.writeByte('\n');
	}
	
	public void write(HTTPHeader header, long value) {		

	    activeTarget.write(header.rootBytes());
	    Appendables.appendValue(activeTarget, value);
		activeTarget.writeByte('\r');
		activeTarget.writeByte('\n');
}

	public void writeUTF8(HTTPHeader header, byte[] value) {

		activeTarget.write(header.rootBytes());
		activeTarget.write(value);
		activeTarget.writeByte('\r');
		activeTarget.writeByte('\n');
}


	/**
	 *
	 * @param header HTTPHeader to append to activeTarget
	 * @param value HeaderValue to append HTTPHeader to
	 */
	public void write(HTTPHeader header, HeaderValue value) {		
	
		    activeTarget.write(header.rootBytes());		
			value.appendTo(activeTarget);
			activeTarget.writeByte('\r');
			activeTarget.writeByte('\n');
	}

	public void write(HTTPHeader header,
			HTTPSpecification<? extends Enum<? extends HTTPContentType>, ? extends Enum<? extends HTTPRevision>, ? extends Enum<? extends HTTPVerb>, ? extends Enum<? extends HTTPHeader>> spec,
			ChannelReader reader) {
    	
		activeTarget.write(header.rootBytes());
    	header.writeValue(activeTarget, spec, reader);
		activeTarget.writeByte('\r');
		activeTarget.writeByte('\n');
	}
	
}
