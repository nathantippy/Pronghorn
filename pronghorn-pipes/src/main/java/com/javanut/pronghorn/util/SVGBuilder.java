package com.javanut.pronghorn.util;

import java.io.IOException;

public class SVGBuilder {
	
	// https://www.w3.org/TR/SVGMobile12/intro.html

	public static Appendable openSVG(Appendable target, int x, int y, int w, int h) {
		try {
			target.append("<?xml version=\"1.0\"?>\n");
			target.append("<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.2\" baseProfile=\"tiny\" viewBox=\"");
			Appendables.appendValue(target,x);
			Appendables.appendValue(target.append(" "),y);
			Appendables.appendValue(Appendables.appendValue(target.append(" "),w)," ",h,"\">\n");

		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		return target;
	}
	
	public static void closeSVG(Appendable target) {
		try {
			target.append("</svg>\n");			
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public static Appendable circle(Appendable target, int x, int y, int r, String fill) {
		try {
			target.append("<circle ");
			Appendables.appendValue(target, "cx='",x,"' ");
			Appendables.appendValue(target, "cy='",y,"' ");
			Appendables.appendValue(target, "r='",r,"' ");
			target.append("fill='").append(fill).append("'");
			target.append("/>\n");
			return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public static Appendable desc(Appendable target, String desc) {
		try {
			return target.append("<desc>").append(desc).append("</desc>\n");
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
	
//	  <rect x="10" y="10" width="10" height="10" fill="red"/>
	
	
//	  <path d='M 10 19 L 15 23 20 19' stroke='black' stroke-width='2'/>  -- fluent until end of path
	
	
	
	
}
