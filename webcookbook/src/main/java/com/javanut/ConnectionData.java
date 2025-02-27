package com.javanut;


import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.pipe.PipeWriter;

public class ConnectionData extends MessageSchema<ConnectionData> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0x90000000,0x80000000,0x80000001,0xc0200004},
		    (short)0,
		    new String[]{"ConnectionData","ConnectionId","SequenceNo","Context",null},
		    new long[]{1, 11, 12, 13, 0},
		    new String[]{"global",null,null,null,null},
		    "ConnectionDataSchema.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ConnectionData() { 
		    super(FROM);
		}

		protected ConnectionData(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ConnectionData instance = new ConnectionData();

		public static final int MSG_CONNECTIONDATA_1 = 0x00000000; //Group/OpenTempl/4
		public static final int MSG_CONNECTIONDATA_1_FIELD_CONNECTIONID_11 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_CONNECTIONDATA_1_FIELD_SEQUENCENO_12 = 0x00000003; //IntegerUnsigned/None/0
		public static final int MSG_CONNECTIONDATA_1_FIELD_CONTEXT_13 = 0x00000004; //IntegerUnsigned/None/1

		public static void consume(Pipe<ConnectionData> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_CONNECTIONDATA_1:
		                consumeConnectionData(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeConnectionData(Pipe<ConnectionData> input) {
		    long fieldConnectionId = PipeReader.readLong(input,MSG_CONNECTIONDATA_1_FIELD_CONNECTIONID_11);
		    int fieldSequenceNo = PipeReader.readInt(input,MSG_CONNECTIONDATA_1_FIELD_SEQUENCENO_12);
		    int fieldContext = PipeReader.readInt(input,MSG_CONNECTIONDATA_1_FIELD_CONTEXT_13);
		}

		public static void publishConnectionData(Pipe<ConnectionData> output, long fieldConnectionId, int fieldSequenceNo, int fieldContext) {
		        PipeWriter.presumeWriteFragment(output, MSG_CONNECTIONDATA_1);
		        PipeWriter.writeLong(output,MSG_CONNECTIONDATA_1_FIELD_CONNECTIONID_11, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_CONNECTIONDATA_1_FIELD_SEQUENCENO_12, fieldSequenceNo);
		        PipeWriter.writeInt(output,MSG_CONNECTIONDATA_1_FIELD_CONTEXT_13, fieldContext);
		        PipeWriter.publishWrites(output);
		}
}
