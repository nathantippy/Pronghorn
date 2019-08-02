package com.javanut.pronghorn.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SVGBuilderTest {

	@Test
	public void testSimple() {
		
		StringBuilder builder = new StringBuilder();
		int x = 1;
		int y = 2;
		int w = 3;
		int h = 4;
		SVGBuilder.closeSVG(SVGBuilder.openSVG(builder, x, y, w, h));
		
		String expected = "<?xml version=\"1.0\"?>\n" + 
				"<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.2\" baseProfile=\"tiny\" viewBox=\"1 2 3 4\">\n</svg>\n";
		
		assertEquals(expected,  builder.toString());
	
		
	}
	
}
