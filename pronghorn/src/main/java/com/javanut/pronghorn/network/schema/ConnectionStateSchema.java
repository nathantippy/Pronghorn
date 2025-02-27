package com.javanut.pronghorn.network.schema;

import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.pipe.PipeWriter;

public class ConnectionStateSchema  extends MessageSchema<ConnectionStateSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400002,0x90000000,0xc0200002},
		    (short)0,
		    new String[]{"State","ArrivalTime","CustomFields",null,"StateNoEcho","ArrivalTime",null},
		    new long[]{1, 12, 14, 0, 2, 12, 0},
		    new String[]{"global",null,null,null,"global",null,null},
		    "ConnectionState.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ConnectionStateSchema() { 
		    super(FROM);
		}

		protected ConnectionStateSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ConnectionStateSchema instance = new ConnectionStateSchema();

		public static final int MSG_STATE_1 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_STATE_1_FIELD_ARRIVALTIME_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_STATE_1_FIELD_CUSTOMFIELDS_14 = 0x01c00003; //ByteVector/None/0
		public static final int MSG_STATENOECHO_2 = 0x00000004; //Group/OpenTempl/2
		public static final int MSG_STATENOECHO_2_FIELD_ARRIVALTIME_12 = 0x00800001; //LongUnsigned/None/0

		public static void consume(Pipe<ConnectionStateSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_STATE_1:
		                consumeState(input);
		            break;
		            case MSG_STATENOECHO_2:
		                consumeStateNoEcho(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeState(Pipe<ConnectionStateSchema> input) {
		    long fieldArrivalTime = PipeReader.readLong(input,MSG_STATE_1_FIELD_ARRIVALTIME_12);
		    DataInputBlobReader<ConnectionStateSchema> fieldCustomFields = PipeReader.inputStream(input, MSG_STATE_1_FIELD_CUSTOMFIELDS_14);
		}
		public static void consumeStateNoEcho(Pipe<ConnectionStateSchema> input) {
		    long fieldArrivalTime = PipeReader.readLong(input,MSG_STATENOECHO_2_FIELD_ARRIVALTIME_12);
		}

		public static void publishState(Pipe<ConnectionStateSchema> output, long fieldArrivalTime, byte[] fieldCustomFieldsBacking, int fieldCustomFieldsPosition, int fieldCustomFieldsLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_STATE_1);
		        PipeWriter.writeLong(output,MSG_STATE_1_FIELD_ARRIVALTIME_12, fieldArrivalTime);
		        PipeWriter.writeBytes(output,MSG_STATE_1_FIELD_CUSTOMFIELDS_14, fieldCustomFieldsBacking, fieldCustomFieldsPosition, fieldCustomFieldsLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishStateNoEcho(Pipe<ConnectionStateSchema> output, long fieldArrivalTime) {
		        PipeWriter.presumeWriteFragment(output, MSG_STATENOECHO_2);
		        PipeWriter.writeLong(output,MSG_STATENOECHO_2_FIELD_ARRIVALTIME_12, fieldArrivalTime);
		        PipeWriter.publishWrites(output);
		}
}
