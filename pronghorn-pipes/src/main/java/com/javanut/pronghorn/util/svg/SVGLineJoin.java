package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.AppendableWriter;

//  miter | round | bevel | inherit	
public enum SVGLineJoin implements AppendableWriter{
	miter,
	round,
	bevel,
	inherit;

	@Override
	public AppendableProxy writeTo(AppendableProxy target) {
		target.append(name());
		return target;
	}
	
	
}
