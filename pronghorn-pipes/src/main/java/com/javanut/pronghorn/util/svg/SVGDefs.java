package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.Appendables;

public class SVGDefs {

	private AppendableProxy target;
	private SVGImage svgImage;
	private SVGLinearGradient linearGradient;
	
	public SVGDefs(AppendableProxy target, SVGImage svgImage) {
		this.target = target;
		this.svgImage = svgImage;
		this.linearGradient = new SVGLinearGradient(target, this);
	}
	
	public final SVGLinearGradient linearGradient(CharSequence id) {
		target.append("<linearGradient xml:id=\"").append(id).append("\">");
		
		return linearGradient;
	}
	


}

