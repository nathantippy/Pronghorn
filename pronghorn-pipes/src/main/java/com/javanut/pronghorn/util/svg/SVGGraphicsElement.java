package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;

public class SVGGraphicsElement {

	private final SVGImage image;
	private final AppendableProxy target;
	
	public SVGGraphicsElement(AppendableProxy target, SVGImage image) {
		this.image = image;
		this.target = target;
	}
	
	public SVGGraphicsElement transform(String transform) {
		target.append("transform=\"").append(transform).append("\" ");
		
		return this;
	}

	public SVGImage nxt() {
		target.append(">");
		
		return image;
	}

	
	
}
