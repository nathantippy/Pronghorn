package com.javanut.pronghorn.util.svg;

import java.awt.Color;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.AppendableWriter;
import com.javanut.pronghorn.util.Appendables;

public class SVGShape {

	private final AppendableProxy target;
	private final SVGImage image;
	
	public SVGShape(AppendableProxy target, SVGImage image) {
		this.target = target;
		this.image = image;
	}
	
	public SVGImage nxt() {
		target.append("/>\n");
		return image;
	}

	public SVGShape fill(String fill) {
		return fill((t)->t.append(fill));
	}
	public SVGShape fill(Color fill) {		
		return fill((t)->Appendables.appendHexDigits(t.append('#'), fill.getRGB()) );
	}
	public SVGShape fill(AppendableWriter fill) {		
		fill.writeTo(target.append(" fill='")).append("'");				
		return this;
	}
	

	public SVGShape stroke(String stroke) {
		return fill((t)->t.append(stroke));
	}
	public SVGShape stroke(Color stroke) {		
		return fill((t)->Appendables.appendHexDigits(t.append('#'), stroke.getRGB()) );
	}
	public SVGShape stroke(AppendableWriter stroke) {		
		stroke.writeTo(target.append(" stroke='")).append("'");				
		return this;
	}
	 
	//TODO: add fill-opacity and stroke-width="3"
	
	
	
}
