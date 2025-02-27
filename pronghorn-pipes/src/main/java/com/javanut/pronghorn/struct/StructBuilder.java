package com.javanut.pronghorn.struct;

import java.util.Arrays;

import com.javanut.json.JSONRequired;
import com.javanut.json.decode.JSONExtractor;
import com.javanut.pronghorn.util.CharSequenceToUTF8;
import com.javanut.pronghorn.util.CharSequenceToUTF8Local;

public class StructBuilder {

	private final static int INIT_SIZE=16;
	
	private final StructRegistry typeData;
		
	private int fieldCount = 0;
	
	private byte[][] fieldNames;
	private StructType[] fieldTypes;
	private int[] fieldDims;
	private Object[] fieldAssoc;
	
	private JSONRequired[] fieldReq;
	private Object[] fieldValid;
	
	
	// type Store Registry,  StructRegistry
	public StructBuilder(StructRegistry typeData) {
		this.typeData = typeData;
		
		this.fieldNames = new byte[INIT_SIZE][];
		this.fieldTypes = new StructType[INIT_SIZE];
		this.fieldDims  = new int[INIT_SIZE];
		this.fieldAssoc  = new Object[INIT_SIZE];
		this.fieldReq = new JSONRequired[INIT_SIZE];
		this.fieldValid = new Object[INIT_SIZE];
		
	}
	
	public StructBuilder(StructRegistry typeData, StructBuilder template) {
		this.typeData = typeData;
		
		this.fieldNames = Arrays.copyOfRange(template.fieldNames, 0, template.fieldCount); 
		this.fieldTypes = Arrays.copyOfRange(template.fieldTypes, 0, template.fieldCount); 
		this.fieldDims  = Arrays.copyOfRange(template.fieldDims, 0, template.fieldCount); 
		this.fieldAssoc = Arrays.copyOfRange(template.fieldAssoc, 0, template.fieldCount); 
		this.fieldReq   = Arrays.copyOfRange(template.fieldReq, 0, template.fieldCount);		
		this.fieldValid = Arrays.copyOfRange(template.fieldValid, 0, template.fieldCount); 
		
	}
	
	public static StructBuilder newStruct(StructRegistry typeData) {
		return new StructBuilder(typeData);
	}
	
	public static StructBuilder newStruct(StructRegistry typeData, StructBuilder template) {
		return new StructBuilder(typeData, template);
	}
	
	public StructBuilder removeLastNFields(int n) {
		fieldCount = Math.max(0, fieldCount-n);
		return this;
	}			
	
	public StructBuilder removeFieldWithName(CharSequence name) {
	
		CharSequenceToUTF8 c = CharSequenceToUTF8Local.get().convert(name);
		
		int i = fieldCount;
		while (--i>=0) {
			byte[] field = fieldNames[i];
			if (c.isEquals(field)) {	
				//move it all down
				fieldCount--;
				//was this field on the end?
				if (i<fieldCount) {
					//value at location i must be removed
					System.arraycopy(fieldNames, i+1, fieldNames, i, fieldCount-i);
					System.arraycopy(fieldTypes, i+1, fieldTypes, i, fieldCount-i);
					System.arraycopy(fieldDims,  i+1, fieldDims,  i, fieldCount-i);
				}					
				return this;
			}
		}
		throw new UnsupportedOperationException("Field "+name+" not found");
	}
	
	public StructBuilder addField(CharSequence fieldName, 
								  StructType fieldType) {
		return addField(fieldName, fieldType, 0, null);
	}
	
	public StructBuilder addField(CharSequence fieldName, 
            StructType fieldType, 
            int fieldDim) {
		return addField(fieldName,fieldType,fieldDim, null);
	}
	
	public StructBuilder addField(CharSequence fieldName, 
            StructType fieldType, 
            Object assoc) {
		return addField(fieldName, fieldType, 0, assoc);
	}

	public <T extends Enum<T>> StructBuilder addField(T fieldObject, StructType fieldType) {
		return addField(fieldObject.name(), fieldType, 0, fieldObject);
	}

	public <T extends Enum<T>> StructBuilder addField(T fieldObject, StructType fieldType, int fieldDim) {
		return addField(fieldObject.name(), fieldType, fieldDim, fieldObject);
	}
	
	public StructBuilder addField(CharSequence fieldName, 
            StructType fieldType, 
            int fieldDim, 
            Object assoc) {
		return addField(fieldName, fieldType, fieldDim, assoc, JSONRequired.OPTIONAL, null);
	};
	
	public StructBuilder addField(CharSequence fieldName, 
			                 StructType fieldType, 
			                 int fieldDim, 
			                 Object assoc, 
			                 JSONRequired isRequired,
			                 Object validator) {
		
		if (fieldCount == fieldTypes.length) {
			fieldNames = grow(fieldNames);
			fieldTypes = grow(fieldTypes);
			fieldDims = grow(fieldDims);
			fieldAssoc = grow(fieldAssoc);
			fieldReq = grow(fieldReq);
		    fieldValid = grow(fieldValid);	
		}
		
		fieldNames[fieldCount] = CharSequenceToUTF8Local.get().convert(fieldName).asBytes();
		fieldTypes[fieldCount] = fieldType;
		fieldDims[fieldCount] = fieldDim;
		fieldAssoc[fieldCount] = assoc;
		fieldReq[fieldCount] = isRequired;
		fieldValid[fieldCount] = validator;
		
		fieldCount++;
		return this;
	}
	
	private JSONRequired[] grow(JSONRequired[] source) {
		JSONRequired[] result = new JSONRequired[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	
	private Object[] grow(Object[] source) {
		Object[] result = new Object[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private int[] grow(int[] source) {
		int[] result = new int[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private StructType[] grow(StructType[] source) {
		StructType[] result = new StructType[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private byte[][] grow(byte[][] source) {
		byte[][] result = new byte[source.length*2][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	public int register() {
		return typeData.addStruct(
				null,
				Arrays.copyOfRange(fieldNames, 0, fieldCount), 
				Arrays.copyOfRange(fieldTypes, 0, fieldCount), 
				Arrays.copyOfRange(fieldDims, 0, fieldCount),
				Arrays.copyOfRange(fieldAssoc, 0, fieldCount),
				Arrays.copyOfRange(fieldReq, 0, fieldCount),
				Arrays.copyOfRange(fieldValid, 0, fieldCount)				
				);		
	}
	
	public int register(Object associated) {
		return typeData.addStruct(
				associated,
				Arrays.copyOfRange(fieldNames, 0, fieldCount), 
				Arrays.copyOfRange(fieldTypes, 0, fieldCount), 
				Arrays.copyOfRange(fieldDims, 0, fieldCount),
				Arrays.copyOfRange(fieldAssoc, 0, fieldCount),
				Arrays.copyOfRange(fieldReq, 0, fieldCount),
				Arrays.copyOfRange(fieldValid, 0, fieldCount)
				
				);		
	}

    public <T extends Enum<T>> StructBuilder longField(T field) {
    	return addField(field, StructType.Long);
    };
	
    public <T extends Enum<T>> StructBuilder integerField(T field) {
    	return addField(field, StructType.Integer);
    };
    
    public <T extends Enum<T>> StructBuilder stringField(T field) {
    	return addField(field, StructType.Text);
    };    
    
    public <T extends Enum<T>> StructBuilder blobField(T field) {
    	return addField(field, StructType.Blob);
    };
    
    public <T extends Enum<T>> StructBuilder decimalField(T field) {
    	return addField(field, StructType.Decimal);
    };
    
    public <T extends Enum<T>> StructBuilder byteField(T field) {
    	return addField(field, StructType.Byte);
    };
    
    public <T extends Enum<T>> StructBuilder rationalField(T field) {
    	return addField(field, StructType.Rational);
    };
    
    public <T extends Enum<T>> StructBuilder booleanField(T field) {
    	return addField(field, StructType.Boolean);
    }; 
	
	
}
