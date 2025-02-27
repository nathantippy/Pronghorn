package com.javanut.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.pronghorn.struct.StructBuilder;
import com.javanut.pronghorn.struct.StructRegistry;
import com.javanut.pronghorn.struct.StructType;
import com.javanut.pronghorn.util.TrieParser;
import com.javanut.pronghorn.util.TrieParserReader;
import com.javanut.pronghorn.util.TrieParserReaderLocal;

public class JSONFieldSchema {

	 private static final Logger logger = LoggerFactory.getLogger(JSONFieldSchema.class);

	 private final TrieParser parser;  //immutable once established

	 private int totalCount;  //immutable once established
	 private int maxPathLength;	  //immutable once established 

	 private JSONFieldMapping[] mappings;  //immutable once established

	 public JSONFieldSchema(int nullPosition) {
		 
		 this.mappings = new JSONFieldMapping[0];
		 
		 this.parser = new TrieParser(256,2,false,true);
		 JSONStreamParser.populateWithJSONTokens(parser);
			 
	 }	 

	 
	 public int mappingCount() {
		 return mappings.length;
	 }
	 
	 public JSONFieldMapping getMapping(int idx) {
		 return mappings[idx];
	 }
	 
	 public void addMappings(JSONFieldMapping mapping) {
		 int newLen = mappings.length+1;
		 JSONFieldMapping[] newArray = new JSONFieldMapping[newLen];
		 System.arraycopy(mappings, 0, 
				 		  newArray, 0, 
				          mappings.length);
		 newArray[mappings.length] = mapping;
		 mappings = newArray;		 
	 }

	 public TrieParser parser() {
		return parser;
	 }
	 
	 public int lookupId(CharSequence text) {
		 //adds new one if it is not found.
		 	
		 long idx = TrieParserReader.query(TrieParserReaderLocal.get(), 
				                           parser, 
				                           text);
		 
		 if (idx < 0) {
			 idx = ++totalCount;
			 
			 int hashVal = JSONStreamParser.toValue((int)idx);
			
			 parser.setUTF8Value(text, hashVal);
			 
			 //This pattern may cause an alt check of string capture
			 //NOTE: this is only true for the keys
			 parser.setUTF8Value("\"", text, "\"", hashVal);
		
			 //logger.info("added token {} with value {} to parser", value, hashVal);
			 
		 } else {
			 idx = JSONStreamParser.fromValue((int)idx);
		 }
		 
		 return (int)idx;
	 }

	//used for moving all the fields down as we generate a hash for this unique path 
	public long maxFieldUnits() {
		return totalCount + maxPathLength;
	}
	
	public int uniqueFieldsCount() {
		return totalCount;
	}

	public void recordMaxPathLength(int length) {
		maxPathLength = Math.max(maxPathLength, length);
	}

	//returns the JSON look up index array
	public int[] addToStruct(StructRegistry struct, int structId) {
				
		int length = mappings.length;
		int[] jsonIndexLookup = new int[length];
						
		int i = length;
		assert(i>0) : "Must not add an empty extraction";
		while (--i>=0) {
			JSONFieldMapping mapping = mappings[i];		
			long fieldId = struct.growStruct(structId, mapTypes(mapping), mapping.dimensions(), mapping.getName().getBytes());
			
			jsonIndexLookup[i] = StructRegistry.FIELD_MASK&(int)fieldId;
			Object assoc = mapping.getAssociatedObject();
			if (null!=assoc) {
				if (!struct.setAssociatedObject(fieldId, assoc)) {
					throw new UnsupportedOperationException("An object with the same identity hash is already held, can not add "+assoc);
				}
			}
			struct.setValidator(fieldId, mapping.isRequired(), mapping.getValidator());
			
		}
		return jsonIndexLookup;
	}

	public int[] indexTable(StructRegistry typeData, int structId) {
		if ((StructRegistry.IS_STRUCT_BIT&structId) == 0  || structId<0) {
			throw new UnsupportedOperationException("invalid structId");
		}		
		
		int[] table = new int[mappings.length];
		
		int t = table.length;
		while(--t>0) {
			long fieldId = typeData.fieldLookup(mappings[t].getName(), structId);
			assert(fieldId!=-1) : "bad field name "+mappings[t].getName()+" not found in struct";
			table[t] = (StructRegistry.FIELD_MASK & (int)fieldId);
		}
		return table;

	}


	public void addToStruct(StructRegistry typeData, StructBuilder structBuilder) {
		int length = mappings.length;
						
		int i = length;
		assert(i>0) : "Must not add an empty extraction";
		while (--i>=0) {
			JSONFieldMapping mapping = mappings[i];		
			structBuilder.addField(mapping.getName(), mapTypes(mapping),  mapping.dimensions(),
					               mapping.getAssociatedObject(),
					               mapping.isRequired(),
					               mapping.getValidator());
			
		}
	}


	private StructType mapTypes(JSONFieldMapping mapping) {
		StructType fieldType = null;
		switch(mapping.type) {
			case TypeString:
				fieldType = StructType.Text;
			break;
			case TypeInteger:
				fieldType = StructType.Long;
			break;
			case TypeDecimal:
				fieldType = StructType.Decimal;
			break;
			case TypeBoolean:
				fieldType = StructType.Boolean;
			break;					
		}
		return fieldType;
	}


	public void debug() {
		// TODO Auto-generated method stub
		for(int i = 0; i<mappings.length; i++) {
			System.out.println("field name:"+mappings[i].getName());
			
		}
	}
	 
	 
}
