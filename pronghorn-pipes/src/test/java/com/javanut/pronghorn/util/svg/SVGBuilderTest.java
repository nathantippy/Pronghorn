package com.javanut.pronghorn.util.svg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import org.junit.Test;

import com.javanut.pronghorn.util.AppendableProxy;

public class SVGBuilderTest {
	
	//////////
	//  https://www.w3.org/TR/SVGMobile12/
	//////////
	
	@Test
	public void testOpenClose() {
		
		StringBuilder builder = new StringBuilder();
		int x = 1;
		int y = 2;
		int w = 3;
		int h = 4;
		
		SVGImage.openSVG(new AppendableProxy(builder), x, y, w, h).closeSVG();
		
		String expected = "<?xml version=\"1.0\"?>\n" + 
				"<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.2\" baseProfile=\"tiny\" viewBox=\"1 2 3 4\">\n</svg>\n";
		
		assertEquals(expected,  builder.toString());	
		
	}
	
	@Test
	public void testCircle() {
		
		StringBuilder builder = new StringBuilder();
		int x = 1;
		int y = 2;
		int w = 3;
		int h = 4;
		
		int cirX = 10;
		int cirY = 11;
		int cirR = 12;
		String fill = "values?";
		
		SVGImage svgImage = SVGImage.openSVG(new AppendableProxy(builder), x, y, w, h);
		svgImage.circle(cirX, cirY, cirR).fill(fill).nxt();
		
		svgImage.closeSVG();
		
		String expected = "<?xml version=\"1.0\"?>\n" + 
				"<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.2\" baseProfile=\"tiny\" viewBox=\"1 2 3 4\">\n"
				+ "<circle cx='10' cy='11' r='12' fill='values?'/>\n"
				+ "</svg>\n";
		
		assertEquals(expected,  builder.toString());	
	}

	//TODO: add tests for all the shapes.	
	
}
