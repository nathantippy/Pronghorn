package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;

public class SVGText {

	private final AppendableProxy target;
	private final SVGImage svgImage;
	
	public SVGText(AppendableProxy target, SVGImage svgImage) {
		this.target = target;
		this.svgImage = svgImage;
	}
	
	public SVGText anchor(String anchor) { //middle
		target.append("text-anchor='").append(anchor).append("' ");
		return this;
	}
	
	public SVGText fontFamily(String family) { //Verdana SVGFreeSansASCII,sans-serif
		target.append("font-family='").append(family).append("' ");
		return this;
	}
	
	
	public SVGText fontSize(String size) { //Verdana SVGFreeSansASCII,sans-serif
		target.append("font-size='").append(size).append("' ");
		return this;
	}
	
	public SVGText fill(String fill) { //Verdana SVGFreeSansASCII,sans-serif
		target.append("fill='").append(fill).append("' ");
		return this;
	}
	
	public SVGImage text(String text) {
		
		target.append(">").append(text).append("</text>\n");
		
		return svgImage;
	}
	
	//TODO: text tspan feature still missing
	//        font-family="SVGFreeSansASCII,sans-serif"    font-family="Verdana" font-size="55" fill="blue"
	//<text x="240" y="70" text-anchor="middle" font-weight="bold" font-size="32">Test  <tspan font-weight="bold" fill="red" >not</tspan> opacity</text>
	
}
