package com.javanut.pronghorn.util.svg;

import com.javanut.pronghorn.util.AppendableProxy;
import com.javanut.pronghorn.util.AppendableWriter;
import com.javanut.pronghorn.util.Appendables;

public final class SVGImage {

	private final AppendableProxy target;
	
	private final SVGShape shape;	
	private final SVGPoints points;
	private final SVGText text;
	private final SVGDefs defs;
	
	public SVGImage(AppendableProxy target) {
		this.target = target;
		this.defs = new SVGDefs(target, this);
		this.text = new SVGText(target, this);
		this.shape = new SVGShape(target, this);
		this.points = new SVGPoints(target, shape);
	}
	
	public SVGImage desc(String desc) {
		return desc( (t)-> t.append(desc) );
	}
	
	public SVGImage desc(AppendableWriter desc) {
		desc.writeTo(target.append("<desc>")).append("</desc>\n");
		return this;
	}


		
		
	
	public final SVGDefs defs() {
		target.append("<defs>");
				
		return defs;
	}
	  
	public final SVGText text(int x, int y) {
		target.append("<text ");
		Appendables.appendValue(target, "x='",x,"' ");
		Appendables.appendValue(target, "y='",y,"' ");
		
		return text;
	}
	
	public final SVGShape circle(int x, int y, int r) {
	
		target.append("<circle ");
		Appendables.appendValue(target, "cx='",x,"' ");
		Appendables.appendValue(target, "cy='",y,"' ");
		Appendables.appendValue(target, "r='",r,"'");
		
		return shape;		
	}
	
	public final SVGShape rect(int x, int y, int width, int height) {
		
		target.append("<rect ");
		Appendables.appendValue(target, "x='",x,"' ");
		Appendables.appendValue(target, "y='",y,"' ");
		Appendables.appendValue(target, "width='",width,"' ");
		Appendables.appendValue(target, "height='",height,"'");
		
		return shape;		
	}

	public final SVGPoints polyline() {
			
			target.append("<polyline points=\"");
			
			return points;		
	}
	
	//////////
	//  https://www.w3.org/TR/SVGMobile12/
	//////////
	
	
	
	//TODO: ellipse
	//TODO: line
	//TODO: polygon
	//TODO: path
	
	
	 //shapes: 'circle', 'ellipse', 'line', 'path', 'polygon', 'polyline' and 'rect'. 
	 
//	<polyline fill="none" stroke="blue" stroke-width="10" 
//            points="50,375
//                    150,375 150,325 250,325 250,375
//                    350,375 350,250 450,250 450,375
//                    550,375 550,175 650,175 650,375
//                    750,375 750,100 850,100 850,375
//                    950,375 950,25 1050,25 1050,375
//                    1150,375" />
	
	
	
	
//	  <rect x="10" y="10" width="10" height="10"      fill="red"/>
//   <rect x="200" y="160" height="20" width="80"    fill="white" fill-opacity="0.5"/>
	
	
//	  <path d='M 10 19 L 15 23 20 19' stroke='black' stroke-width='2'/>  -- fluent until end of path
	
	
  
	public final void closeSVG() {
		
		target.append("</svg>\n");			
	
	}

	public static SVGImage openSVG(AppendableProxy target, int x, int y, int w, int h) {
		
		target.append("<?xml version=\"1.0\"?>\n");
		target.append("<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.2\" baseProfile=\"tiny\" viewBox=\"");
		Appendables.appendValue(target,x);
		Appendables.appendValue(target.append(" "),y);
		Appendables.appendValue(Appendables.appendValue(target.append(" "),w)," ",h,"\">\n");
	
		return new SVGImage(target);
	}
	
}
