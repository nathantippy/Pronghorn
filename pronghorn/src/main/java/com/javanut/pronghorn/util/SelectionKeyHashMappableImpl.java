package com.javanut.pronghorn.util;

import com.javanut.pronghorn.network.SelectionKeyHashMappable;

public class SelectionKeyHashMappableImpl implements SelectionKeyHashMappable {

	private int skPos = -1;
	
	@Override
	public void skPosition(int position) {
		skPos=position;
	}

	@Override
	public int skPosition() {
		return skPos;
	}
	
}
