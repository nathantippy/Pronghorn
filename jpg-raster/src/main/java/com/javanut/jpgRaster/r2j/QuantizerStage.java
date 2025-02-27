package com.javanut.jpgRaster.r2j;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.jpgRaster.JPG;
import com.javanut.jpgRaster.JPGSchema;
import com.javanut.jpgRaster.JPG.Header;
import com.javanut.jpgRaster.JPG.MCU;
import com.javanut.jpgRaster.JPG.QuantizationTable;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.DataOutputBlobWriter;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.pipe.PipeWriter;
import com.javanut.pronghorn.stage.PronghornStage;
import com.javanut.pronghorn.stage.scheduling.GraphManager;

/**
 * Performs quantization on a JPG schema pipe and returns
 * the results back onto a JPG schema pipe.
 * Examples can be found here:
 * <a href="http://scc.ustc.edu.cn/zlsc/sugon/intel/ipp/ipp_manual/IPPS/ipps_ch9/ch9_lp_analysis_and_quantization_functions.htm">Quantization</a>
 */
public class QuantizerStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(QuantizerStage.class);
			
	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	private boolean verbose;
	private int quality;
	
	private Header header;
	private MCU mcu;

	/**
	 *
	 * @param graphManager
	 * @param input _in_ The JPG schema on which quantization will be applied to
	 * @param output _out_ JPG schema with applied quantization
	 * @param verbose
	 * @param quality
	 */
	public QuantizerStage(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output, boolean verbose, int quality) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.quality = quality;

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	}
	
	@Override
	public void startup() {
		mcu = new MCU();
	}
	
	private static void quantizeMCU(short[] MCU, QuantizationTable table) {
		for (int i = 0; i < MCU.length; ++i) {
			MCU[JPG.zigZagMap[i]] = (short)(MCU[JPG.zigZagMap[i]] / table.table[i]);
		}
	}
	
	public static void quantize(MCU mcu, int quality) {
		if (quality == 50) {
			quantizeMCU(mcu.y, JPG.qTable0_50);
			quantizeMCU(mcu.cb, JPG.qTable1_50);
			quantizeMCU(mcu.cr, JPG.qTable1_50);
		}
		else if (quality == 75) {
			quantizeMCU(mcu.y, JPG.qTable0_75);
			quantizeMCU(mcu.cb, JPG.qTable1_75);
			quantizeMCU(mcu.cr, JPG.qTable1_75);
		}
		else {
			quantizeMCU(mcu.y, JPG.qTable0_100);
			quantizeMCU(mcu.cb, JPG.qTable1_100);
			quantizeMCU(mcu.cr, JPG.qTable1_100);
		}
		return;
	}

	@Override
	public void run() {
		long s = System.nanoTime();
		while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				int last = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401);
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					if (verbose) {
						System.out.println("Quantizer writing header to pipe...");
					}
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, header.filename);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, last);
					PipeWriter.publishWrites(output);
				}
				else {
					logger.error("Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_4) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
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
				
				quantize(mcu, quality);
				//JPG.printMCU(mcu);

				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_4)) {
					DataOutputBlobWriter<JPGSchema> mcuWriter = PipeWriter.outputStream(output);
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.y[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cb[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CB_204);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cr[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CR_304);
					
					PipeWriter.publishWrites(output);
				}
				else {
					logger.error("Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else {
				logger.error("Quantizer requesting shutdown");
				requestShutdown();
			}
		}
		timer.addAndGet(System.nanoTime() - s);
	}
	
	public static AtomicLong timer = new AtomicLong(0);//NOTE: using statics like this is not recommended
	
}
