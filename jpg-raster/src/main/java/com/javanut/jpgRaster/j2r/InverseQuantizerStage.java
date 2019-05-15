package com.javanut.jpgRaster.j2r;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.jpgRaster.JPG;
import com.javanut.jpgRaster.JPGSchema;
import com.javanut.jpgRaster.JPG.ColorComponent;
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
 * Performs inverse quantization on a JPG schema pipe and returns
 * the results back onto a JPG schema pipe.
 * Examples can be found here:
 * <a href="http://scc.ustc.edu.cn/zlsc/sugon/intel/ipp/ipp_manual/IPPI/ippi_ch16/ch16_inverse_quantization.htm">Inverse Quantization</a>
 */
public class InverseQuantizerStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(InverseQuantizerStage.class);
			
	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	boolean verbose;
	
	Header header;
	MCU mcu;

	/**
	 *
	 * @param graphManager
	 * @param input _in_ Input JPG schema
	 * @param output _out_ Output JPG schema
	 * @param verbose
	 */
	public InverseQuantizerStage(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	}
	
	@Override
	public void startup() {
		mcu = new MCU();
	}
	
	private static void dequantizeMCU(short[] MCU, QuantizationTable table) {
		for (int i = 0; i < MCU.length; ++i) {
			// type casting might be unsafe for 16-bit precision quantization tables
			MCU[JPG.zigZagMap[i]] = (short)(MCU[JPG.zigZagMap[i]] * table.table[i]);
		}
	}
	
	public static void dequantize(MCU mcu, Header header) {
		dequantizeMCU(mcu.y, header.quantizationTables[header.colorComponents[0].quantizationTableID]);
		if (header.numComponents > 1) {
			dequantizeMCU(mcu.cb, header.quantizationTables[header.colorComponents[1].quantizationTableID]);
			dequantizeMCU(mcu.cr, header.quantizationTables[header.colorComponents[2].quantizationTableID]);
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
						System.out.println("Inverse Quantizer writing header to pipe...");
					}
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, header.filename);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, last);
					PipeWriter.publishWrites(output);
				}
				else {
					logger.error("Inverse Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_COLORCOMPONENTMESSAGE_2) {
				// read color component data from pipe
				ColorComponent component = new ColorComponent();
				component.componentID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
				component.horizontalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
				component.verticalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
				component.quantizationTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
				header.colorComponents[component.componentID - 1] = component;
				header.numComponents += 1;
				PipeReader.releaseReadLock(input);
				
				// write color component data to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
					if (verbose) {
						System.out.println("Inverse Quantizer writing color component to pipe...");
					}
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, component.componentID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, component.horizontalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, component.verticalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, component.quantizationTableID);
					PipeWriter.publishWrites(output);
				}
				else {
					logger.error("Inverse Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3) {
				// read quantization table from pipe
				QuantizationTable table = new QuantizationTable();
				table.tableID = (short) PipeReader.readInt(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLEID_103);
				table.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_PRECISION_203);

				DataInputBlobReader<JPGSchema> r = PipeReader.inputStream(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLE_303);
				for (int i = 0; i < 64; ++i) {
					table.table[i] = r.readInt();
				}
				
				PipeReader.releaseReadLock(input);
				
				// tableID always reads as 0 for some reason
				// this is a workaround
				int i = 0;
				while (header.quantizationTables[i] != null) {
					i += 1;
				}
				header.quantizationTables[i] = table;
				//header.quantizationTables[table.tableID] = table;
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
				
				dequantize(mcu, header);

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
					logger.error("Inverse Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else {
				logger.error("Inverse Quantizer requesting shutdown");
				requestShutdown();
			}
		}
		timer.addAndGet(System.nanoTime() - s);
	}
	
	public static AtomicLong timer = new AtomicLong(0);//NOTE: using statics like this is not recommended
	
}
