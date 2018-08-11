package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines a typical networked response. Includes payload, context, connection, host, port, and more.
 */
public class NetResponseSchema extends MessageSchema<NetResponseSchema> {


	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0x90000000,0x80000000,0xb8000000,0xc0200004,0xc0400004,0x90000000,0x80000000,0xb8000000,0xc0200004,0xc0400003,0xa8000001,0x80000000,0xc0200003},
		    (short)0,
		    new String[]{"Response","ConnectionId","ContextFlags","Payload",null,"Continuation","ConnectionId",
		    "ContextFlags","Payload",null,"Closed","Host","Port",null},
		    new long[]{101, 1, 5, 3, 0, 102, 1, 5, 3, 0, 10, 4, 5, 0},
		    new String[]{"global",null,null,null,null,"global",null,null,null,null,"global",null,null,null},
		    "NetResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});

    
    protected NetResponseSchema() {
        super(FROM);
    }
         
    public static final NetResponseSchema instance = new NetResponseSchema();
    
    public static final int MSG_RESPONSE_101 = 0x00000000; //Group/OpenTempl/4
    public static final int MSG_RESPONSE_101_FIELD_CONNECTIONID_1 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_RESPONSE_101_FIELD_PAYLOAD_3 = 0x01c00004; //ByteVector/None/0
    
    public static final int MSG_CONTINUATION_102 = 0x00000005; //Group/OpenTempl/4
    public static final int MSG_CONTINUATION_102_FIELD_CONNECTIONID_1 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_CONTINUATION_102_FIELD_CONTEXTFLAGS_5 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_CONTINUATION_102_FIELD_PAYLOAD_3 = 0x01c00004; //ByteVector/None/0
    
    public static final int MSG_CLOSED_10 = 0x0000000a; //Group/OpenTempl/3
    public static final int MSG_CLOSED_10_FIELD_HOST_4 = 0x01400001; //UTF8/None/1
    public static final int MSG_CLOSED_10_FIELD_PORT_5 = 0x00000003; //IntegerUnsigned/None/0

    public static void consume(Pipe<NetResponseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RESPONSE_101:
                    consumeResponse(input);
                break;
                case MSG_CONTINUATION_102:
                    consumeContinuation(input);
                break;
                case MSG_CLOSED_10:
                    consumeClosed(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeResponse(Pipe<NetResponseSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_RESPONSE_101_FIELD_CONNECTIONID_1);
        int fieldContextFlags = PipeReader.readInt(input,MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5);
        DataInputBlobReader<NetResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_RESPONSE_101_FIELD_PAYLOAD_3);
    }
    public static void consumeContinuation(Pipe<NetResponseSchema> input) {
        long fieldConnectionId = PipeReader.readLong(input,MSG_CONTINUATION_102_FIELD_CONNECTIONID_1);
        int fieldContextFlags = PipeReader.readInt(input,MSG_CONTINUATION_102_FIELD_CONTEXTFLAGS_5);
        DataInputBlobReader<NetResponseSchema> fieldPayload = PipeReader.inputStream(input, MSG_CONTINUATION_102_FIELD_PAYLOAD_3);
    }
    public static void consumeClosed(Pipe<NetResponseSchema> input) {
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_CLOSED_10_FIELD_HOST_4,new StringBuilder(PipeReader.readBytesLength(input,MSG_CLOSED_10_FIELD_HOST_4)));
        int fieldPort = PipeReader.readInt(input,MSG_CLOSED_10_FIELD_PORT_5);
    }

    public static void publishResponse(Pipe<NetResponseSchema> output, long fieldConnectionId, int fieldContextFlags, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_RESPONSE_101);
            PipeWriter.writeLong(output,MSG_RESPONSE_101_FIELD_CONNECTIONID_1, fieldConnectionId);
            PipeWriter.writeInt(output,MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5, fieldContextFlags);
            PipeWriter.writeBytes(output,MSG_RESPONSE_101_FIELD_PAYLOAD_3, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishContinuation(Pipe<NetResponseSchema> output, long fieldConnectionId, int fieldContextFlags, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_CONTINUATION_102);
            PipeWriter.writeLong(output,MSG_CONTINUATION_102_FIELD_CONNECTIONID_1, fieldConnectionId);
            PipeWriter.writeInt(output,MSG_CONTINUATION_102_FIELD_CONTEXTFLAGS_5, fieldContextFlags);
            PipeWriter.writeBytes(output,MSG_CONTINUATION_102_FIELD_PAYLOAD_3, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishClosed(Pipe<NetResponseSchema> output, CharSequence fieldHost, int fieldPort) {
            PipeWriter.presumeWriteFragment(output, MSG_CLOSED_10);
            PipeWriter.writeUTF8(output,MSG_CLOSED_10_FIELD_HOST_4, fieldHost);
            PipeWriter.writeInt(output,MSG_CLOSED_10_FIELD_PORT_5, fieldPort);
            PipeWriter.publishWrites(output);
    }
}
