package com.ociweb.jpgRaster;

import com.ociweb.jpgRaster.JPG.ColorComponent;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BMPDumper extends PronghornStage {

	private final Pipe<JPGSchema> input;
	
	Header header;
	String filename;
	MCU mcu = new MCU();
	
	short[][] pixels;
	int count;
	int mcuHeight;
	int mcuWidth;
	int numMCUs;
	int pos;
	
	protected BMPDumper(GraphManager graphManager, Pipe<JPGSchema> input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	public static void Dump(short[][] pixels, String filename) throws IOException {
		int width = pixels[0].length / 3;
		int height = pixels.length;
		int paddingSize = (4 - (width * 3) % 4) % 4;
		int size = 14 + 12 + pixels.length * pixels[0].length + height * paddingSize;
		
		FileOutputStream fileStream = new FileOutputStream(filename);
		FileChannel file = fileStream.getChannel();
		
		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.put((byte) 'B');
		buffer.put((byte) 'M');
		putInt(buffer, size);
		putInt(buffer, 0);
		putInt(buffer, 0x1A);
		putInt(buffer, 12);
		putShort(buffer, width);
		putShort(buffer, height);
		putShort(buffer, 1);
		putShort(buffer, 24);
		
		for (int i = height - 1; i >= 0; --i) {
			for (int j = 0; j < width * 3 - 2; j += 3) {
				buffer.put((byte)(pixels[i][j + 2] & 0xFF));
				buffer.put((byte)(pixels[i][j + 1] & 0xFF));
				buffer.put((byte)(pixels[i][j + 0] & 0xFF));
			}
			for (int j = 0; j < paddingSize; j++) {
				buffer.put((byte)0);
			}
		}
		buffer.flip();
		while(buffer.hasRemaining()) {
			file.write(buffer);
		}
		file.close();
		fileStream.close();
	}
	
	private static void putInt(ByteBuffer buffer, int v) throws IOException {
		buffer.put((byte)(v & 0xFF));
		buffer.put((byte)((v >> 8) & 0xFF));
		buffer.put((byte)((v >> 16) & 0xFF));
		buffer.put((byte)((v >> 24) & 0xFF));
	}
	
	private static void putShort(ByteBuffer buffer, int v) throws IOException {
		buffer.put((byte)(v & 0xFF));
		buffer.put((byte)((v >> 8) & 0xFF));
	}
	
	public void copyPixels(int mcuNum) {
		int curPixelY = (mcuNum / mcuWidth) * 8;
		int curPixelX = (mcuNum % mcuWidth) * 8;
		for (int i = curPixelY; i < curPixelY + 8; ++i) {
			for (int j = curPixelX; j < curPixelX + 8; ++j) {
				if (i < header.height && j < header.width) {
					pixels[i][j * 3 + 0] = mcu.y[(i % 8) * 8 + (j % 8)];
					pixels[i][j * 3 + 1] = mcu.cb[(i % 8) * 8 + (j % 8)];
					pixels[i][j * 3 + 2] = mcu.cr[(i % 8) * 8 + (j % 8)];
				}
			}
		}
	}

	@Override
	public void run() {
		
		while (PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				header.frameType = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, new StringBuilder()).toString();
				header.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501);
				header.startOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601);
				header.endOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701);
				header.successiveApproximationLow = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801);
				PipeReader.releaseReadLock(input);

				pixels = new short[header.height][header.width * 3];
				count = 0;
				mcuHeight = (header.height + 7) / 8;
				mcuWidth = (header.width + 7) / 8;
				numMCUs = mcuHeight * mcuWidth;
				pos = 0;
			}
			else if (msgIdx == JPGSchema.MSG_COLORCOMPONENTMESSAGE_2) {
				// read color component data from pipe
				ColorComponent component = new ColorComponent();
				component.componentID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
				component.horizontalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
				component.verticalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
				component.quantizationTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
				component.huffmanACTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502);
				component.huffmanDCTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602);
				header.colorComponents[component.componentID - 1] = component;
				header.numComponents += 1;
				PipeReader.releaseReadLock(input);
				if (component.componentID == 1 && component.horizontalSamplingFactor == 2 &&
					((header.width - 1) / 8 + 1) % 2 == 1) {
					mcuWidth += 1;
				}
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_6) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106);
				for (int i = 0; i < 64; ++i) {
					mcu.y[i] = mcuReader.readShort();
				}
				
				for (int i = 0; i < 64; ++i) {
					mcu.cb[i] = mcuReader.readShort();
				}
				
				for (int i = 0; i < 64; ++i) {
					mcu.cr[i] = mcuReader.readShort();
				}
				PipeReader.releaseReadLock(input);

				copyPixels(pos);
				
				count += 1;
				
				if (header.colorComponents[0].verticalSamplingFactor == 2 &&
					mcuHeight > 1) {
					if (pos % (mcuWidth * 2) == mcuWidth * 2 - 1) {
						pos += 1;
					}
					else if (pos < count) {
						pos += mcuWidth;
					}
					else {
						pos -= mcuWidth - 1;
					}
				}
				else {
					pos = count;
				}
				
				if (count >= numMCUs) {
					try {
						int extension = filename.lastIndexOf('.');
						if (extension == -1) {
							filename += ".bmp";
						}
						else {
							filename = filename.substring(0, extension) + ".bmp";
						}
						System.out.println("Writing to " + filename + " ...");
						Dump(pixels, filename);
						System.out.println("Done.");
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			else {
				System.err.println("BMPDumper requesting shutdown");
				requestShutdown();
			}
		}
	
	}
	
	/*public static void main(String[] args) {
		
		byte[][] rgb = new byte[8][8 * 3];
		// red
		rgb[0][0 * 3 + 0] = (byte)255;
		rgb[0][0 * 3 + 1] = 0;
		rgb[0][0 * 3 + 2] = 0;
		// green
		rgb[0][1 * 3 + 0] = 0;
		rgb[0][1 * 3 + 1] = (byte)255;
		rgb[0][1 * 3 + 2] = 0;
		// blue
		rgb[0][2 * 3 + 0] = 0;
		rgb[0][2 * 3 + 1] = 0;
		rgb[0][2 * 3 + 2] = (byte)255;
		// cyan
		rgb[1][0 * 3 + 0] = 0;
		rgb[1][0 * 3 + 1] = (byte)255;
		rgb[1][0 * 3 + 2] = (byte)255;
		// magenta
		rgb[1][1 * 3 + 0] = (byte)255;
		rgb[1][1 * 3 + 1] = 0;
		rgb[1][1 * 3 + 2] = (byte)255;
		// yellow
		rgb[1][2 * 3 + 0] = (byte)255;
		rgb[1][2 * 3 + 1] = (byte)255;
		rgb[1][2 * 3 + 2] = 0;
		// black
		rgb[2][0 * 3 + 0] = 0;
		rgb[2][0 * 3 + 1] = 0;
		rgb[2][0 * 3 + 2] = 0;
		// gray
		rgb[2][1 * 3 + 0] = (byte)128;
		rgb[2][1 * 3 + 1] = (byte)128;
		rgb[2][1 * 3 + 2] = (byte)128;
		// white
		rgb[2][2 * 3 + 0] = (byte)255;
		rgb[2][2 * 3 + 1] = (byte)255;
		rgb[2][2 * 3 + 2] = (byte)255;
		try {
			Dump(rgb, 3, 3, "bmp_test.bmp");
		} catch (IOException e) {
			System.out.println("Error - Unknown error creating BMP file");
		}
		
	}*/
}
