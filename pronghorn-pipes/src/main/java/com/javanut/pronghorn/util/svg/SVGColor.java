package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.AppendableWriter;

public enum SVGColor implements AppendableWriter {

	black,
	silver,
	gray,
	white,
	maroon,
	red,
	purple,
	fuchsia,
	green,
	lime,
	olive,
	yellow,
	navy,
	blue,
	teal,
	aqua;

	@Override
	public AppendableProxy writeTo(AppendableProxy target) {
		return target.append(name());
	}	
	
}
