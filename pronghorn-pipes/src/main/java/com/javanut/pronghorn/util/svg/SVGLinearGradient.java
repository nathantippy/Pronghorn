package com.javanut.pronghorn.util.svg;

import java.awt.Color;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.Appendables;

public class SVGLinearGradient {

	private AppendableProxy target;
	private SVGDefs svgDefs;
	boolean opacityAdjusted = false;
	
	
	public SVGLinearGradient(AppendableProxy target, SVGDefs svgDefs) {
		this.target = target;
		this.svgDefs = svgDefs;
	}

	public SVGLinearGradient stop(long mOffset, byte eOffset, Color stopColor) {
		return stop(mOffset,eOffset, stopColor, 0,(byte)0);
	}
	
	public SVGLinearGradient stop(long mOffset, byte eOffset, Color stopColor, long mStopOpacity, byte eStopOpacity) {
		target.append("<stop offset=\"");		
		Appendables.appendDecimalValue(target, mOffset, eOffset).append("\" ");
		Appendables.appendFixedHexDigitsRaw(target.append(" stop-color=\"").append('#'), stopColor.getRGB() & 0x00FFFFFF,24).append("\"");
				
		if (mStopOpacity>0 || opacityAdjusted) {
			opacityAdjusted = true;
			Appendables.appendDecimalValue(target.append("stop-opacity=\""), mStopOpacity, eStopOpacity).append("\" ");
		}
		target.append("\">");
		return this;
	}
	
	public SVGDefs end() {
		opacityAdjusted = false;
		return svgDefs;
	}
	
	//TODO: add gradent to be used for end of poly.	
//  <defs>
//  <linearGradient xml:id="MyGradient">
//    <stop offset="0.05" stop-color="#F60"/>
//    <stop offset="0.95" stop-color="#FF6"/>
//	<stop offset="0" stop-color="#F00" stop-opacity="0"/>
	//<stop offset="1" stop-color="#0F0" stop-opacity="1"/>
//  </linearGradient>
//</defs>
	
	
}
