package com.ociweb.pronghorn.util.field;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.util.TrieParserReader;

public class MessageConsumer {

	List<FieldConsumer> list = new ArrayList<FieldConsumer>();
	TrieParserReader reader = new TrieParserReader();
	FieldConsumer[][] consumers = new FieldConsumer[4][]; //how many for all the fields?
	
	public MessageConsumer() {
				
	}
	
    private void storeField(int fieldId, FieldConsumer fc) {
    	
    	if (fieldId >= consumers.length) {
    		
    		int newLength = Math.max(consumers.length*2,fieldId+1);    		
    		FieldConsumer[][] newConsumers = new FieldConsumer[newLength][];    		
    		System.arraycopy(consumers, 0, newConsumers, 0, consumers.length);    		
    		consumers = newConsumers;
    		
    	}
    	
    	FieldConsumer[] fieldConsumers = consumers[fieldId];    	
    	int len = null==fieldConsumers?0:fieldConsumers.length;
    	FieldConsumer[] fieldConsumersTemp = new FieldConsumer[len+1];
    	if (len>0) {
    		System.arraycopy(fieldConsumers, 0, fieldConsumersTemp, 0, len);
    	}
    	fieldConsumersTemp[len] = fc;
    	consumers[fieldId] = fieldConsumersTemp;
    	
    }

    public boolean process(ChannelReader reader) {
    	storeFields(reader);
    	return consumeFields();
    }
    
    
	private void storeFields(ChannelReader reader) {
		
		//These asserts are required to ensure no one refactors the TypeMask to modify 
		//the order value of these constants.
		assert(TypeMask.IntegerSigned   == 0x02);// integer
		assert(TypeMask.LongSigned      == 0x06);// integer
		assert(TypeMask.TextUTF8        == 0x0A);// bytes 
		assert(TypeMask.Decimal         == 0x0C);// decimal
		assert(TypeMask.ByteVector      == 0x0E);// bytes
		assert(TypeMask.Rational        == 0x1A);// rational
				
		while (reader.hasRemainingBytes()) {

			//////////////
			//read type
			////////////
			int token = reader.readPackedInt();
			
			int type = TokenBuilder.extractType(token);
			
			int fieldId = TokenBuilder.extractId(token);
			
			///////////
			//read name length
			//////////
			int fieldNameLength = reader.readShort();
			assert(fieldNameLength == -1);
			
			//////////
			//consume type
			//////////
			FieldConsumer[] localConsumers = consumers[fieldId];
			int i;
			if (null != localConsumers && ((i=localConsumers.length)>0)) {
				
				int p = DataInputBlobReader.absolutePosition((DataInputBlobReader)reader);//keep this position so we can roll-back reader each time.
				while (--i >= 0) {
					DataInputBlobReader.absolutePosition((DataInputBlobReader)reader,p);
					storeValue(reader, type, localConsumers[i]);
				}
			} else {
				skipValue(reader, type);
			}

		}		
	}
		
	private boolean consumeFields() {
		for(int i = 0; i<list.size(); i++) {
			if (!list.get(i).run()) {
				return false;
			}
		}
		return true;
	}

	private void storeValue(ChannelReader reader, int type, FieldConsumer consumer) {
		
		//NB: must not use more than 3 conditionals to find any specific value.
		//NB: the order of these are from most common to least common
		if (type < 0x0B) {
			if (type < 0x0A) {
					//integers
				    consumer.store(reader.readPackedLong());
			} else {
				
				Pipe backingPipe = DataInputBlobReader.getBackingPipe((DataInputBlobReader)reader);
				short length = reader.readShort();	
				
				consumer.store(backingPipe.blobRing, DataInputBlobReader.absolutePosition((DataInputBlobReader)reader), length, backingPipe.blobMask);
				if (length>0) {
					reader.skipBytes(length);
				}
			}			
		} else {
			//>=0x0B
			if (type == TypeMask.ByteVector) {
				// bytes
				
				Pipe backingPipe = DataInputBlobReader.getBackingPipe((DataInputBlobReader)reader);
				short length = reader.readShort();	
	
				consumer.store(backingPipe.blobRing, DataInputBlobReader.absolutePosition((DataInputBlobReader)reader), length, backingPipe.blobMask);			
				if (length>0) {
					reader.skipBytes(length);
				}
			} else {
				if (type == TypeMask.Decimal) {
					//decimal
					consumer.store(reader.readByte(), reader.readPackedLong());
				} else {
					//rational
					consumer.store(reader.readPackedLong(), reader.readPackedLong());
				}
			}
		}
	}
	
	private void skipValue(ChannelReader reader, int type) {
		
		//NB: must not use more than 3 conditionals to find any specific value.
		//NB: the order of these are from most common to least common
		if (type < 0x0B) {
			if (type < 0x0A) {
					//integers
				    reader.readPackedLong();
			} else {				
				short length = reader.readShort();					
				if (length>0) {
					reader.skipBytes(length);
				}
			}			
		} else {
			//>=0x0B
			if (type == TypeMask.ByteVector) {
				// bytes
				
				short length = reader.readShort();				
				if (length>0) {
					reader.skipBytes(length);
				}
			} else {
				if (type == TypeMask.Decimal) {
					//decimal
					reader.readByte();
					reader.readPackedLong();
				} else {
					//rational
					reader.readPackedLong();
					reader.readPackedLong();
				}
			}
		}
	}
	
	public <A extends Appendable> MessageConsumer utf8Processor(int fieldId, UTF8FieldProcessor<A> processor, A target) {
		
		FieldConsumer consumer = new UTF8FieldConsumer<A>(processor, target);		
		storeField(fieldId,consumer);
		list.add(consumer);
		
		return this;
	}
	
	public MessageConsumer bytesProcessor(int fieldId, BytesFieldProcessor processor) {
		
		FieldConsumer consumer = new BytesFieldConsumer(processor);		
		storeField(fieldId,consumer);
		list.add(consumer);
		
		return this;
	}
	
    public MessageConsumer decimalProcessor(int fieldId, DecimalFieldProcessor processor) {
    	
    	FieldConsumer consumer = new DecimalFieldConsumer(processor, reader);
    	storeField(fieldId,consumer);
		list.add(consumer);
		
		return this;
	}

    public MessageConsumer integerProcessor(int fieldId, IntegerFieldProcessor processor) {
    	
    	FieldConsumer consumer = new IntFieldConsumer(processor, reader);
    	storeField(fieldId,consumer);
		list.add(consumer);
		
		return this;
	}
    
    public MessageConsumer rationalProcessor(int fieldId, RationalFieldProcessor processor) {
    	
    	FieldConsumer consumer = new RationalFieldConsumer(processor, reader);
    	storeField(fieldId,consumer);
		list.add(consumer);
		
		return this;
	}
	
}
