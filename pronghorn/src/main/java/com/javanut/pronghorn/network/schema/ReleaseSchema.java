package com.javanut.pronghorn.network.schema;

import com.javanut.pronghorn.pipe.FieldReferenceOffsetManager;
import com.javanut.pronghorn.pipe.MessageSchema;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.PipeReader;
import com.javanut.pronghorn.pipe.PipeWriter;

/**
 * Defines release messages. These are acknowledgments to be sent back to another stage to let them know
 * that a pipe is free or a task has finished. Use Position and SequenceNo fields to indicate where operation
 * has ceased.
 */
public class ReleaseSchema extends MessageSchema<ReleaseSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0x90000001,0xc0200003,0xc0400005,0x90000000,0x90000001,0x88000000,0x88000001,0xc0200005},
		    (short)0,
		    new String[]{"Release","ConnectionID","Position",null,"ReleaseWithSeq","ConnectionID","Position",
		    "SequenceNo","PipeIdx",null},
		    new long[]{100, 1, 2, 0, 101, 1, 2, 3, 4, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,null,null},
		    "Release.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ReleaseSchema() { 
		    super(FROM);
		}

		protected ReleaseSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ReleaseSchema instance = new ReleaseSchema();

		public static final int MSG_RELEASE_100 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_RELEASE_100_FIELD_CONNECTIONID_1 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_RELEASE_100_FIELD_POSITION_2 = 0x00800003; //LongUnsigned/None/1
		public static final int MSG_RELEASEWITHSEQ_101 = 0x00000004; //Group/OpenTempl/5
		public static final int MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2 = 0x00800003; //LongUnsigned/None/1
		public static final int MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3 = 0x00400005; //IntegerSigned/None/0
		public static final int MSG_RELEASEWITHSEQ_101_FIELD_PIPEIDX_4 = 0x00400006; //IntegerSigned/None/1

		public static void consume(Pipe<ReleaseSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_RELEASE_100:
		                consumeRelease(input);
		            break;
		            case MSG_RELEASEWITHSEQ_101:
		                consumeReleaseWithSeq(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeRelease(Pipe<ReleaseSchema> input) {
		    long fieldConnectionID = PipeReader.readLong(input,MSG_RELEASE_100_FIELD_CONNECTIONID_1);
		    long fieldPosition = PipeReader.readLong(input,MSG_RELEASE_100_FIELD_POSITION_2);
		}
		public static void consumeReleaseWithSeq(Pipe<ReleaseSchema> input) {
		    long fieldConnectionID = PipeReader.readLong(input,MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1);
		    long fieldPosition = PipeReader.readLong(input,MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2);
		    int fieldSequenceNo = PipeReader.readInt(input,MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3);
		    int fieldPipeIdx = PipeReader.readInt(input,MSG_RELEASEWITHSEQ_101_FIELD_PIPEIDX_4);
		}

		public static void publishRelease(Pipe<ReleaseSchema> output, long fieldConnectionID, long fieldPosition) {
		        PipeWriter.presumeWriteFragment(output, MSG_RELEASE_100);
		        PipeWriter.writeLong(output,MSG_RELEASE_100_FIELD_CONNECTIONID_1, fieldConnectionID);
		        PipeWriter.writeLong(output,MSG_RELEASE_100_FIELD_POSITION_2, fieldPosition);
		        PipeWriter.publishWrites(output);
		}
		public static void publishReleaseWithSeq(Pipe<ReleaseSchema> output, long fieldConnectionID, long fieldPosition, int fieldSequenceNo, int fieldPipeIdx) {
		        PipeWriter.presumeWriteFragment(output, MSG_RELEASEWITHSEQ_101);
		        PipeWriter.writeLong(output,MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1, fieldConnectionID);
		        PipeWriter.writeLong(output,MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2, fieldPosition);
		        PipeWriter.writeInt(output,MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3, fieldSequenceNo);
		        PipeWriter.writeInt(output,MSG_RELEASEWITHSEQ_101_FIELD_PIPEIDX_4, fieldPipeIdx);
		        PipeWriter.publishWrites(output);
		}
}
