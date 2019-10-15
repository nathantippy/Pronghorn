package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.Appendables;

public class SVGPoints {

	private AppendableProxy target;
	private SVGShape shape;
	
	public SVGPoints(AppendableProxy target, SVGShape shape) {
		this.target = target;
		this.shape = shape;
	}
		
	public SVGPoints point(int x, int y) {
		Appendables.appendValue(Appendables.appendValue(target, shape.boundX(x)).append(","),shape.boundY(y)).append(" ");
		return this;
	}
	
	public SVGShape pathEnd() {
		target.append("\"");
		return shape;
	}

}
