package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.MCU;

public class InverseDCT {
	public static short[] MCUInverseDCT(short[] mcu) {
		/*System.out.print("Before Inverse DCT:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(mcu[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		short[] result = new short[64];
		
		for (int y = 0; y < 8; ++y) {
			for (int x = 0; x < 8; ++x) {
				double sum = 0.0;
				for (int i = 0; i < 8; ++i) {
					for (int j = 0; j < 8; ++j) {
						double Cu = 1.0;
						double Cv = 1.0;
						if (i == 0) {
							Cv = 1 / Math.sqrt(2.0);
						}
						if (j == 0) {
							Cu = 1 / Math.sqrt(2.0);
						}
						sum += Cu * Cv * mcu[i * 8 + j] *
							   Math.cos((2.0 * x + 1.0) * j * Math.PI / 16.0) *
							   Math.cos((2.0 * y + 1.0) * i * Math.PI / 16.0);
					}
				}
				sum /= 4.0;
				result[y * 8 + x] = (short)sum;
			}
		}
		
		/*System.out.print("After Inverse DCT:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(result[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		return result;
	}
	
	public static void inverseDCT(ArrayList<MCU> mcus) {
		for (int i = 0; i < mcus.size(); ++i) {
			mcus.get(i).y =  MCUInverseDCT(mcus.get(i).y);
			mcus.get(i).cb = MCUInverseDCT(mcus.get(i).cb);
			mcus.get(i).cr = MCUInverseDCT(mcus.get(i).cr);
		}
		return;
	}
	
	public static void main(String[] args) {
		short[] mcu = new short[] {
				-252, -36,  -5, -6, 15, -4, 6, 0,
				  55,  84, -14,  7,  0,  0, 0, 0,
				  20,   0, -18, -9,  0,  0, 0, 0,
				 -24,  32,   0,  0,  0,  0, 0, 0,
				 22,  -22,   0,  0,  0,  0, 0, 0,
				  0,    0,   0,  0,  0,  0, 0, 0,
				  0,    0,   0,  0,  0,  0, 0, 0,
				  0,    0,   0,  0,  0,  0, 0, 0
		};
		short[] result = MCUInverseDCT(mcu);
		for (int i = 0; i < 8; i++) {
			for (int j = 0; j < 8; j++) {
				System.out.print(String.format("%04d ", result[i * 8 + j]));
			}
			System.out.println();
		}
	}
}
