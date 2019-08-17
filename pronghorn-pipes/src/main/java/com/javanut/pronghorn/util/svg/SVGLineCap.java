package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.AppendableWriter;

//stroke-linecap     butt | round | square | inherit
public enum SVGLineCap implements AppendableWriter {
	butt,
	round,
	square,
	inherit;

	@Override
	public AppendableProxy writeTo(AppendableProxy target) {		
		target.append(name());		
		return target;
	}
}
