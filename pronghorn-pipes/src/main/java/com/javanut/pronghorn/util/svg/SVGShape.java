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
		return fill((t)->{
			return Appendables.appendFixedHexDigitsRaw(t.append('#'), fill.getRGB() & 0x00FFFFFF,24);
		} );
	}
	public SVGShape fill(AppendableWriter fill) {		
		fill.writeTo(target.append(" fill='")).append("'");				
		return this;
	}
	

	public SVGShape stroke(String stroke) {
		return stroke((t)->t.append(stroke));
	}
	public SVGShape stroke(Color stroke) {		
		return stroke((t)->Appendables.appendFixedHexDigitsRaw(t.append('#'), stroke.getRGB() & 0x00FFFFFF,24) );
	}
	public SVGShape stroke(AppendableWriter stroke) {		
		stroke.writeTo(target.append(" stroke='")).append("'");				
		return this;
	}
	 
	public SVGShape fillOpacity(final long opacityM, final int opacityE) {
		assert(opacityE<64);
		assert(opacityE>-64);		
		return fillOpacity((t)->Appendables.appendDecimalValue(t, opacityM, (byte)opacityE));
	}
	public SVGShape fillOpacity(AppendableWriter opacity) {		
		opacity.writeTo(target.append(" fill-opacity='")).append("'");				
		return this;
	}

	public SVGShape strokeOpacity(final long opacityM, final int opacityE) {
		assert(opacityE<64);
		assert(opacityE>-64);		
		return strokeOpacity((t)->Appendables.appendDecimalValue(t, opacityM, (byte)opacityE));
	}
	public SVGShape strokeOpacity(AppendableWriter opacity) {		
		opacity.writeTo(target.append(" stroke-opacity='")).append("'");				
		return this;
	}
	
	//  https://www.w3.org/TR/SVGMobile12/painting.html
		
	public SVGShape strokeLineCap(SVGLineCap lineCap) {		
		lineCap.writeTo(target.append(" stroke-linecap='")).append("'");				
		//stroke-linecap     butt | round | square | inherit			
		return this;
	}
	
	
	// stroke-miterlimit
	public SVGShape strokeMiterLimit(final int miterLimit) {		
		return strokeMiterLimit((t)->Appendables.appendValue(t, miterLimit));
	}
	public SVGShape strokeMiterLimit(AppendableWriter opacity) {		
		opacity.writeTo(target.append(" stroke-miterlimit='")).append("'");				
		return this;
	}	
	
	public SVGShape strokeLineJoin(SVGLineJoin lineJoin) {		
		lineJoin.writeTo(target.append(" stroke-linejoin='")).append("'");				
		//stroke-linejoin     miter | round | bevel | inherit		
		return this;
	}	
		
	
	public SVGShape strokeWidth(int w) {
		return strokeWidth((t)-> 
				Appendables.appendValue(t,w)				
				);
	}
	public SVGShape strokeWidth(AppendableWriter width) {
		width.writeTo(target.append(" stroke-width='")).append("'");
		return this;
	}
	
	
	
	
	
}
