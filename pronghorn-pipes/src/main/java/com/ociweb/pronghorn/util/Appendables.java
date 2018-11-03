package com.ociweb.pronghorn.util;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

/**
 * 
 * Garbage free single pass utilities for building up text.
 * The API follows a fluent pattern where every method returns the same Appendable which was passed in.
 * 
 * TODO: as soon we we start building this code for Java 8 we muse use new UncheckedIOException(cause) instead of new RuntimeException();
 * 
 * @author Nathan Tippy
 *
 */
public class Appendables {
    
	private final static Logger logger = LoggerFactory.getLogger(Appendables.class);
	
    private final static char[] hBase = new char[] {'0','1','2','3','4','5','6','7','8','9',
    												'a','b','c','d','e','f'};
    
    
    private final static char[] base64 = new char[]{'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P',
    		                                        'Q','R','S','T','U','V','W','X','Y','Z','a','b','c','d','e','f',
    		                                        'g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v',
    		                                        'w','x','y','z','0','1','2','3','4','5','6','7','8','9','+','/'};
    
    private final static byte[] base64Inverse;
    
    static {
    	//populate inverse for base64 decode
    	base64Inverse = new byte[256];    	
    	Arrays.fill(base64Inverse, (byte)-1);
    	int i = base64.length;
    	while (--i>=0) {
    		base64Inverse[(int)	base64[i] ] = (byte)i;
    	}
    }

    
    
    public static <A extends Appendable> A appendArray(A target, char left, long[] a, char right) {
    	try {
	        if (a != null) {        
	            int iMax = a.length - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,a[i]);
	                //target.append(Long.toString(a[i]));
	                if (i == iMax) {
	                    return (A) target.append(right);
	                }
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    
    public static <A extends Appendable> A appendArray(A target, byte[] b) {
   		return appendArray(target, '[', b, ']', b.length);
    }
    
    public static <A extends Appendable> A appendArray(A target, char left, byte[] b, char right) {
   		return appendArray(target, left, b, right, b.length);
    }
        
    public static <A extends Appendable> A appendArray(A target, char left, byte[] b, char right, int bLength) {
		try {
		    	if (b != null) {        
		            int iMax = bLength - 1;
		            if (iMax == -1) {
		                target.append(left).append(right);
		                return target;
		            }
		            target.append(left);
		            for (int i = 0; ; i++) {
		                appendValue(target,b[i]);
		                //target.append(Integer.toString(a[i]));
		                if (i == iMax)
		                    return (A) target.append(right);
		                target.append(", ");
		            } 
		        } else {
		            target.append("null");        
		        }
		        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendArray(A target, byte[] b, int offset, int mask, int bLength) {
    	return appendArray(target,'[',b,offset,mask,']',bLength);
    }
    
    public static <A extends Appendable> A appendArray(A target, char left, byte[] b, int offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,b[mask & (i+offset) ]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendHexArray(A target, char left, byte[] b, int offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	            	
	            	appendFixedHexDigits(target, 0xFF&b[mask & (i+offset) ], 8);
	                //appendValue(target,b[mask & (i+offset) ]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    

    public static <A extends Appendable> A appendArray(A target, char left, int[] b, long offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,b[mask & (int)(i+offset) ]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
   }
   
    public static <A extends Appendable> A appendHexArray(A target, char left, int[] b, long offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	            	appendFixedHexDigits(target, 0xFFFFFFFF&b[mask & (int)(i+offset) ], 32);

	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
 }
    
    public static <A extends Appendable> A appendArray(A target, char left, int[] a, char right) {
	     try {
	    	if (a != null) {        
	            int iMax = a.length - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,a[i]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendArray(A target, char left, Object[] a, char right) {
	     try{
	    	if (a != null) {        
	            int iMax = a.length - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                target.append((a[i]).toString());
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, int value, CharSequence suffix) {
    	try {
	        appendValue(target,label, value);
	        target.append(suffix);
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, int value) {
    	try {
	        target.append(label);
	        return appendValue(target,value);
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    static final int digits = 18;    		
    static final long tens = 1_000_000_000_000_000_000L;
    
    
    static final int digits_small = 6;   
    static final long tens_small = 1_000_000L;
    static final long tens_small_limit = tens_small*10;

    public static <A extends Appendable> A appendDecimalValue(A target, long m, byte e) {
    	
    	
    	long value = m;    	
    	int g = -e;
    	boolean useParensForNeg = false;
    	
    	try {    		
	        
	        boolean isNegative = value<0;
	        if (isNegative) {
	        	if (useParensForNeg) {
	        		target.append("(-");
	        	} else {
	        		target.append("-");
	        	}
	        	
	            value = -value;
	        }
	        
	        long nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        
	        long temp = Math.abs(value);
		    if (temp<tens_small_limit) {	
		    	//TODO: move this instance of to the top??
		    	if (target instanceof ChannelWriter) {
		    		decimalValueCollecting((ChannelWriter)target, digits_small, tens_small, g, nextValue, orAll);
		    	} else {
		    		decimalValueCollecting(target, digits_small, tens_small, g, nextValue, orAll);
		    	}
	        } else {
	        	if (target instanceof ChannelWriter) {
	        		decimalValueCollecting((ChannelWriter)target, digits, tens, g, nextValue, orAll);
	        	} else {
	        		decimalValueCollecting(target, digits, tens, g, nextValue, orAll);
	        	}	
	        }
	       
	        
	        int f = e;
	        f = appendZeros(target, f);	    
	        
	        if (isNegative && useParensForNeg) {
	        	target.append(')');
	        }
	        
	        return target;
	        
	        
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

	private static <A extends Appendable> int appendZeros(A target, int f) throws IOException {
		while (f>0) {
			target.append('0');
			f--;
		}
		return f;
	}

	
	private static char[]dv = new char[] {'0','1','2','3','4','5','6','7','8','9'};
	
	
	private static <A extends Appendable> void decimalValueCollecting(A target, int digits, long tens, int g,
																		long nextValue, int orAll) throws IOException {
		
		boolean isFirst = true;
		while (tens>1) {
			
		    int digit = (int)(nextValue/tens);
		    nextValue = nextValue%tens;
		    orAll |= digit;
		    if (0!=orAll || digits<g) {
		        target.append(dv[digit]);//(char)('0'+digit));
		        isFirst = false;
		    }
		    
		    if (digits == g) {
		    	if (isFirst) {
		    		target.append('0');//leading zero
		    	}
		    	target.append('.');
		    	isFirst = false;
		    }
		    
		    tens /= 10;
		    digits--;
		    
		}
		target.append(dv[(int)nextValue]);//(char)('0'+nextValue));
	}
	
	private static <A extends Appendable> void decimalValueCollecting(ChannelWriter writer, int digits, long tens, int g,
			long nextValue, int orAll) throws IOException {

		boolean isFirst = true;
		while (tens > 1) {

			int digit = (int) (nextValue / tens);
			nextValue = nextValue % tens;
			orAll |= digit;
			if (0 != orAll || digits < g) {
				writer.writeByte(dv[digit]);// (char)('0'+digit));
				isFirst = false;
			}

			if (digits == g) {
				if (isFirst) {
					writer.writeByte('0');// leading zero
				}
				writer.writeByte('.');
				isFirst = false;
			}

			tens /= 10;
			digits--;

		}
		writer.writeByte(dv[(int) nextValue]);// (char)('0'+nextValue));
	}
    
    public static <A extends Appendable> A appendValue(A target, int value) {    	
    	return appendValue(target, value, true);
    }

	public static <A extends Appendable> A appendValue(final A target, int value, final boolean useNegPara) {
				
		//////////////////////////////
		//can be optimized due to knowing the target type
		//////////////////////////////
		if (target instanceof AppendableByteWriter) {
			AppendableByteWriter dataOutputBlobWriter = (AppendableByteWriter)target;
			if (value>=0 ) {	    
				if (value<10) {
					dataOutputBlobWriter.writeByte(('0'+(int)value));
					return target;
				} else if (value<100) {
					dataOutputBlobWriter.writeByte(('0'+((int)value/10)));
					dataOutputBlobWriter.writeByte(('0'+((int)value%10)));
					return target;
				} else if (value<1000) {
					dataOutputBlobWriter.writeByte(('0'+((int)value/100)));
					dataOutputBlobWriter.writeByte(('0'+(((int)value%100)/10)));
					dataOutputBlobWriter.writeByte(('0'+((int)value%10)));
					return target;
				} else if (value<10000) {
					dataOutputBlobWriter.writeByte(('0'+((int)value/1000)));
					dataOutputBlobWriter.writeByte(('0'+(((int)value%1000)/100)));
					dataOutputBlobWriter.writeByte(('0'+(((int)value%100)/10)));
					dataOutputBlobWriter.writeByte(('0'+((int)value%10)));
					return target;
				}
			}
			if (dataOutputBlobWriter instanceof DataOutputBlobWriter) {			
				DataOutputBlobWriter.appendLongAsText((DataOutputBlobWriter)dataOutputBlobWriter, value, useNegPara);
			}  else {
				if (dataOutputBlobWriter instanceof AppendableBuilder) {
					AppendableBuilder.appendLongAsText((AppendableBuilder)dataOutputBlobWriter, value, useNegPara);
				} else {
					return slowAppendValue(target, value, useNegPara); //TODO: speed up as other methods.. 
				}
			}
	    	return target;
		} else {		
			return slowAppendValue(target, value, useNegPara);
		}
	}


	private static <A extends Appendable> A slowAppendValue(final A target, int value, final boolean useNegPara) {
		try {
		    int tens = 1000000000;
		    
		    boolean isNegative = value<0;
		    if (isNegative) {
		        //special case which can not be rendered here.
		        if (value==Integer.MIN_VALUE) {
		            return appendValue(target,(long)value);
		        }
		        if (useNegPara) {
		        	target.append('(');
		        }
		        target.append('-');
		        value = -value;
		    }
		    
		    int nextValue = value;
		    int orAll = 0; //this is to remove the leading zeros
		    while (tens>1) {
		        int digit = nextValue/tens;
		        orAll |= digit;
		        if (0!=orAll) {
		            target.append((char)('0'+digit));
		        }
		        nextValue = nextValue%tens;
		        tens /= 10;
		    }
		    target.append((char)('0'+nextValue));
		    if (isNegative && useNegPara) {
		        target.append(')');
		    }
		    return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
	}
    
    public static <A extends Appendable> A appendHexDigits(A target, int value) {
     try{
    	return (A) appendHexDigitsRaw(target.append("0x"), value);
     } catch (IOException ex) {
		throw new RuntimeException(ex); 
     }
    }
    
    public static <A extends Appendable> A appendHexDigitsRaw(A target, int value) {
		try {
		        int bits = 32 - Integer.numberOfLeadingZeros(value);
		        
		        //round up to next group of 4
		        bits = ((bits+3)>>2)<<2;
		        
		        int nextValue = value;
		        int orAll = 0; //this is to remove the leading zeros
		        while (bits>4) {
		            bits -= 4;            
		            int digit = nextValue>>>bits;
		            orAll |= digit;
		            if (0!=orAll) {
		                target.append(hBase[digit]);            
		            }
		            nextValue =  ((1<<bits)-1) & nextValue;
		        }
		        bits -= 4;
		        target.append(hBase[nextValue>>>bits]);
		        
		        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, long value, CharSequence suffix) {
	    try {	
	        appendValue(target,label, value);
	        target.append(suffix);
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, long value) {
    	try {
    		target.append(label);
    		return appendValue(target,value);
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static int appendedLength(long value) {
    	int result = value<0?3:0;
    	value = Math.abs(value);
    	
    	while (value > 0) {
    		result++;
    		value = value/10;
    	}
    	
    	return result;
    }
    
  
    
    
    public static <A extends Appendable> A appendValue(A target, long value) {
    	return customNegAppendValue(target, value, true);
    }

    public static <A extends Appendable> A appendValue(A target, long value, boolean useNegPara) {
    	return customNegAppendValue(target, value, useNegPara);
    }

    
	private static <A extends Appendable> A customNegAppendValue(A target, long value, boolean useNegPara) {
		
		if (target instanceof AppendableByteWriter){
			AppendableByteWriter dataOutputBlobWriter = (AppendableByteWriter)target;
			//////////////////////////////
			//can be optimized due to knowing the target type
			//////////////////////////////
	    	if (value>=0) {	    
	    		if (value<10) {
					dataOutputBlobWriter.writeByte(('0'+(int)value));
	    			return target;
	    		} else if (value<100) {
	    			dataOutputBlobWriter.writeByte(('0'+((int)value/10)));
	    			dataOutputBlobWriter.writeByte(('0'+((int)value%10)));
	    			return target;
	    		} else if (value<1000) {
	    			dataOutputBlobWriter.writeByte(('0'+((int)value/100)));
	    			dataOutputBlobWriter.writeByte(('0'+(((int)value%100)/10)));
	    			dataOutputBlobWriter.writeByte(('0'+((int)value%10)));
	    			return target;
	    		} else if (value<10000) {
	    			dataOutputBlobWriter.writeByte(('0'+((int)value/1000)));
	    			dataOutputBlobWriter.writeByte(('0'+(((int)value%1000)/100)));
	    			dataOutputBlobWriter.writeByte(('0'+(((int)value%100)/10)));
	    			dataOutputBlobWriter.writeByte(('0'+((int)value%10)));
	    			return target;
	    		}
	    		
	    	}
	    	if (dataOutputBlobWriter instanceof DataOutputBlobWriter) {			
				DataOutputBlobWriter.appendLongAsText((DataOutputBlobWriter)dataOutputBlobWriter, value, useNegPara);
			}  else {
				if (dataOutputBlobWriter instanceof AppendableBuilder) {
					AppendableBuilder.appendLongAsText((AppendableBuilder)dataOutputBlobWriter, value, useNegPara);
				} else {
					return slowAppendValue(target, value, useNegPara); //TODO: speed up as other methods.. 
				}
			}
	    	return target;
		} else {
	    	/////////////////////////////    	
	    	return slowAppendValue(target, value, useNegPara);
		}
	}

	private static <A extends Appendable> A slowAppendValue(A target, long value, boolean useNegPara) {
		try {
	        long tens = 1000000000000000000L;
	        
	        boolean isNegative = value<0;
	        if (isNegative) {
	        	if (useNegPara) {
	        		target.append('(');
	        	}
	            target.append('-');
	            value = -value;
	        }
	        
	        long nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        while (tens>1) {
	            int digit = (int)(nextValue/tens);
	            nextValue = nextValue%tens;
	            orAll |= digit;
	            if (0!=orAll) {
	            	target.append((char)('0'+digit));	            
	            }
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        if (isNegative && useNegPara) {
	            target.append(')');
	        }
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
	}
    
    public static <A extends Appendable> A appendHexDigits(A target, long value) {
        try{
        	return (A) appendHexDigitsRaw(target.append("0x"), value);
        } catch (IOException ex) {
        	throw new RuntimeException(ex); 
        }
     }
    
    public static <A extends Appendable> A appendHexDigitsRaw(A target, long value) {
        try {
	        int bits = 64-Long.numberOfLeadingZeros(value);
	        
	        //round up to next group of 4
	        bits = ((bits+3)>>2)<<2;
	
	        long nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        while (bits>4) {
	            bits -= 4;            
	            int digit = (int)(0xF&(nextValue>>>bits));
	            orAll |= digit;
	            if (0!=orAll) {
	                target.append(hBase[digit]);            
	            }
	            nextValue =  ((1L<<bits)-1L) & nextValue;
	        }
	        bits -= 4;
	        target.append(hBase[(int)(nextValue>>>bits)]);
	        
	        return target;
    	} catch (IOException ex) {
    		throw new RuntimeException(ex); 
    	}
    }
    
    public static <A extends Appendable> A appendFixedHexDigits(A target, long value, int bits) {
    	
    	value = value & ((1L<<bits)-1L);//we want only the lowest bits
    	
    	try {
	        //round up to next group of 4
	        bits = ((bits+3)>>2)<<2;
	        
	        target.append("0x");
	        long nextValue = value;
	        while (bits>4) {            
	            bits -= 4;
	            target.append(hBase[(int)(0xF&(nextValue>>>bits))]);            
	            nextValue =  ((1L<<bits)-1L) & nextValue;
	        }
	        bits -= 4;
	        target.append(hBase[(int)(0xF&(nextValue>>>bits))]);
	        
	        return target;
    	} catch (IOException ex) {
    		throw new RuntimeException(ex); 
    	}
    }
    
    /*
     * 
     * In order to render a number like 42 with exactly 2 places the tests argument must be set to 10, likewise 042 would require 100
     */
    public static <A extends Appendable> A appendFixedDecimalDigits(A target, int value, int tens) {

    	try {
	        if (value<0) {
	            target.append('-');
	            value = -value;
	        }
	        
	        int nextValue = value;
	        while (tens>1) {
	            target.append((char)('0'+(nextValue/tens)));
	            nextValue = nextValue%tens;
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        
	        return target;
    	} catch (IOException ex) {
    		
    		throw new RuntimeException(ex); 
    	}
    }
    
    public static <A extends Appendable> A appendFixedDecimalDigits(A target, long value, int tens) {

    	try {
	        if (value<0) {
	            target.append('-');
	            value = -value;
	        }
	        
	        long nextValue = value;
	        while (tens>1) {
	            target.append((char)('0'+(nextValue/tens)));
	            nextValue = nextValue%tens;
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        
	        return target;
    	} catch (IOException ex) {
    		
    		throw new RuntimeException(ex); 
    	}
    }
    
    /*
     * 
     * In order to render an 8 bit number the bits must be set to 8. note that bits can only be in units of 4.
     */
    public static <A extends Appendable> A appendFixedHexDigits(A target, int value, int bits) {

    	//value = value & ((1<<bits)-1);//we want only the lowest bits
    	
    	try {
	        //round up to next group of 4
	        bits = ((bits+3)>>2)<<2;
	        
	        target.append("0x");
	        int nextValue = value;
	        while (bits>4) {            
	            bits -= 4;
	            target.append(hBase[nextValue>>>bits]);            
	            nextValue =  ((1<<bits)-1) & nextValue;
	        }
	        bits -= 4;
	        target.append(hBase[nextValue>>>bits]);
	        
	        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    
    public static <A extends Appendable> A appendClass(A target, Class clazz, Class clazzParam) {
    	try {
    		return (A) target.append(clazz.getSimpleName()).append('<').append(clazzParam.getSimpleName()).append("> ");
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendStaticCall(A target, Class clazz, String method) {
    	try {
    		return (A) target.append(clazz.getSimpleName()).append('.').append(method).append('(');
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static StringBuilder truncate(StringBuilder builder) {
        builder.setLength(0);
        return builder;
    }

    //copy the sequence but skip every instance of the provided skip chars
    public static <A extends Appendable> A appendAndSkip(A target, CharSequence source, CharSequence skip) {
        return appendAndSkipImpl(target, source, skip, skip.length(), source.length(), 0, 0);    
    }

    private static <A extends Appendable> A appendAndSkipImpl(A target, CharSequence source, CharSequence skip, int skipLen, int sourceLen, int j, int i) {

	        for(; i<sourceLen; i++) {            
	            if (source.charAt(i)!=skip.charAt(j)) {
	                copyChars(target, source, j, 0, i-j);
	                j=0;
	            } else {
	                if (skipLen == ++j) {
	                    j=0;
	                }
	            }        
	        }
	        return target;
    }

    private static void copyChars(Appendable target, CharSequence source, int j, int k, int base) {
    	try {
	        for(; k<=j ; k++) {                    
	            target.append(source.charAt(base+k));
	        }
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }


    // + IS %2B
    // / IS %2F
    // = IS %3D    
    
    
    //TODO: add unit tests for each of the cases listed here https://en.wikipedia.org/wiki/Base64
    
    /**
     * Writes URL encoded base64 encoded value of range found in backing array.
     * 
     */
    public static <A extends Appendable> A appendBase64Encoded(A target, byte[] backing, int pos, int len, int mask) {
        //  https://en.wikipedia.org/wiki/Base64
    	try {
	    	int accumulator = 0;
	    	int i = 0;
	    	int shift = -6;
	    	int count = 0;
	    	while (i < len) {
	    		
	    		shift+=8; // 2 4 (we now have 10)
	    		accumulator = (accumulator<<8) | (0xFF&backing[mask & pos++]);
	    		i++;
	    
				while (shift >= 0) {
					int index = 0x3F&(accumulator>>shift);
					
					if (index<62) {
						target.append(base64[index]);
					} else {
						if (index==62) {
							assert(base64[index]=='+');
							target.append("%2B");
						} else {
							assert(base64[index]=='/');
							target.append("%2F");
						}
					}
		
					shift -= 6; //took top 6 now shift is at -4, 
					count++;
				}   		
	    	}
	    	
	    	if (shift<0) {//last letter.
	    		
	    		shift+=8; 
	    		accumulator = (accumulator<<8) | (0xFF&0);
	    		i++;
	    		
	    		while (shift > 0) {    			
	    			int index = 0x3F&(accumulator>>shift);
					
					if (index<62) {
						target.append(base64[index]);
					} else {
						if (index==62) {
							assert(base64[index]=='+');
							target.append("%2B");
						} else {
							assert(base64[index]=='/');
							target.append("%2F");
						}
					}
	    			shift -= 6; //took top 6 now shift is at -4,
	    			count++;
	    		} 
	    		
	    	}
	    	//NOTE: could and should be optimized.
	        while ((count & 0x03) != 0) {
	        	target.append("%3D");
	        	count++;
	        }
	    
	    	return target;
    	} catch (IOException ioex) {
    		throw new RuntimeException(ioex);
    	}
    }
    
    ///ERROR, this method may add an extra A=== on the end... TODO: urgent need test and fix method.
    
    @Deprecated
    public static <A extends Appendable> A appendBase64(A target, byte[] backing, int pos, int len, int mask) {
        //  https://en.wikipedia.org/wiki/Base64
    	try {
    		
    		assert(Integer.lowestOneBit(mask+1) == mask+1) : "mask must be all ones but found "+Integer.toBinaryString(mask);
    		
	    	int accumulator = 0;
	    	int i = 0;
	    	int shift = -6;
	    	int count = 0;
	    	while (i < len) {
	    		
	    		shift+=8; // 2 4 (we now have 10)
	    		accumulator = (accumulator<<8) | (0xFF&backing[mask & pos++]);
	    		i++;
	    
				while (shift >= 0) {
					target.append(base64[0x3F&(accumulator>>shift)]);
					shift -= 6; //took top 6 now shift is at -4, 
					count++;
				}   		
	    	}
	    	
	    	if (shift<0) {//last letter.
	    		
	    		shift+=8; 
	    		accumulator = (accumulator<<8) | (0xFF&0);
	    		i++;
	    		
	    		while (shift > 0) {    			
	    			target.append(base64[0x3F&(accumulator>>shift)]);
	    			shift -= 6; //took top 6 now shift is at -4,
	    			count++;
	    		} 
	    		
	    	}
	    	//NOTE: could and should be optimized.
	        while ((count & 0x03) != 0) {
	        	target.append('=');
	        	count++;
	        }
	    
	    	return target;
    	} catch (IOException ioex) {
    		throw new RuntimeException(ioex);
    	}
    }
        

	public static int decodeBase64(byte[] source, int sourceIdx, int sourceLen, int sourceMask, 
								   byte[] target, int targetIdx, int targetMask) {

		int accumulator = 0;
		int bitsAvail = 0;
		int t = targetIdx;
		int i = sourceLen;
		while (--i >= 0) {

			byte b = base64Inverse[source[sourceMask & sourceIdx++]];
			if (b >= 0) {

				// roll in 6 bits at a time
				accumulator = (accumulator << 6) | (0x3F & b);
				bitsAvail += 6;

				while (bitsAvail > 8) {

					bitsAvail -= 8;
					target[targetMask & t++] = (byte) (accumulator >> bitsAvail);

				}
				// Note the xtra bits < 8 are not used on the very last cycle

			} else {
				if (i > 0) {
					logger.info("invlid value found in base64 encoding");
					return -1; // can not decode
				} else {
					// last block, nothing to do.
				}
			}
		}

		return t - targetIdx;

	}
    public static <A extends Appendable> A appendUTF8(A target, byte[] backing, int pos, int len, int mask) {
    	try {
    		assert(len>=0) : "length: "+len;
    		assert((mask == Integer.MAX_VALUE) || (len<Integer.MAX_VALUE-mask));
	        long localPos = mask&pos;//to support streams longer than 32 bits
	        long charAndPos = ((long)localPos)<<32;
	        long limit = ((long)localPos+len)<<32;
	
	        while (charAndPos<limit) {
	            charAndPos = Pipe.decodeUTF8Fast(backing, charAndPos, mask);
	            target.append((char)charAndPos);
	        }
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
   
    public static boolean isEqualUTF8(byte[] backing, int pos, int len, int mask, CharSequence target) {
    	int tPos = 0;
    	
		assert(len>=0) : "length: "+len;
		assert((mask == Integer.MAX_VALUE) || (len<Integer.MAX_VALUE-mask));
        long localPos = mask&pos;//to support streams longer than 32 bits
        long charAndPos = ((long)localPos)<<32;
        long limit = ((long)localPos+len)<<32;

        while (charAndPos < limit) {
            charAndPos = Pipe.decodeUTF8Fast(backing, charAndPos, mask);
            if (tPos>=target.length() || target.charAt(tPos++) != ((char)charAndPos) ) {
            	return false;
            }
        }
        return true;
	
    }
    
	public static CharSequence[] split(CharSequence text, char c) {
		return split(0,0,0,text,c);
	}
	
	private static CharSequence[] split(int pos, int start, int depth, final CharSequence text, final char c) {
		CharSequence[] result;
		while (pos<text.length()) {
			if (text.charAt(pos++)==c) {
				result = split(pos, pos ,depth+1, text, c);
				result[depth] = text.subSequence(start, pos-1);
				return result;
			}
		}
		result = new CharSequence[depth+1];
		result[depth] = text.subSequence(start, text.length());		
		return result;
	}
	
	public static AppendableProxy proxy(Appendable a) {
		return new AppendableProxy(a);
	}
	
	public static AppendableProxy wrap(Appendable a) {
		return new AppendableProxy(a);
	}
	
	public static AppendableProxy join(Appendable ... targets) {
		if (targets.length==1) {
			return new AppendableProxy(targets[0]);
		} else {
			return new AppendablesProxy(targets);
		}
	}

	public static <A extends Appendable> A  appendEpochTime(A target, long msTime) {
		 try {
			 appendFixedDecimalDigits(
			 appendFixedDecimalDigits(
			 appendFixedDecimalDigits(
					 appendValue(target, (msTime/(60L*60_000L))).append(':')
					  ,(msTime/60_000L)%60L,10).append(':')
			          ,(msTime/1000L)%60L,10).append('.')
			          ,msTime%1000L,100);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		 
		 
		 return target;
	}

	public static <A extends Appendable> A appendNearestTimeUnit(A target, long nsValue, String postfix) {
		appendNearestTimeUnit(target, nsValue);
		try {
			target.append(postfix);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return target;
	}

	public static <A extends Appendable> A appendNearestTimeUnit(A target, long nsValue) {
		try {
			if (nsValue<7_000) {
				appendFixedDecimalDigits(target, nsValue, 1000).append(" ns");
			} else if (nsValue<7_000_000){
				appendFixedDecimalDigits(target, nsValue/1_000,1000).append(" µs");
			} else if (nsValue<7_000_000_000L){
				appendFixedDecimalDigits(target, nsValue/1_000_000,1000).append(" ms");				
			} else if (nsValue<90_000_000_000L){
				appendFixedDecimalDigits(target, nsValue/1_000_000_000L,100).append(" sec");
			} else if (nsValue<(120L*60_000_000_000L)){
				appendFixedDecimalDigits(target, nsValue/60_000_000_000L,100).append(" min");
			} else {
				appendValue(target, nsValue/(60L*60_000_000_000L)).append(" hr");
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return target;
	}

	private static final String[] htmlEntities = buildHTMLEntities();
	private static final byte[][] httpEntitiesUTF8 = encodeUTF8(buildHTMLEntities());
	
	/**
	 * @param target Appendable for encoded data
	 * @param source CharSequence source text to be encoded
	 * @return the target Apppendable for more data to be added.
	 */
	public static <A extends Appendable> A appendHTMLEntityEscaped(A target, CharSequence source) {	
		if (target instanceof AppendableByteWriter) {
			appendHTMLEntityEscaped1(source, (AppendableByteWriter)target);
			return target;
		} else {
			return appendHTMLEntityEscaped2(target, source);
		}
	}

	private static void appendHTMLEntityEscaped1(final CharSequence source, final AppendableByteWriter abw) {
		final int len = source.length();
		for(int i = 0; i<len; i++) {
			char at = source.charAt(i);
			byte[] entity = null;
			if (at>=64 || null == (entity = httpEntitiesUTF8[(int)at])) {
				abw.append(at);
			} else {
				abw.write(entity);
			}
		}
	}


	private static <A extends Appendable> A appendHTMLEntityEscaped2(A target, CharSequence source) {
		try {
			String entity = null;
			for(int i = 0; i<source.length(); i++) {
				char at = source.charAt(i);
				if (at>=64 || null == (entity = htmlEntities[(int)at])) {
					target.append(at);
				} else {
					target.append(entity);					
				}
			}		
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
				
		return target;
	}


	private static byte[][] encodeUTF8(String[] input) {
		byte[][] result = new byte[input.length][];
		int i = input.length;
		while (--i>=0) {
			result[i] = null==input[i]?null:input[i].getBytes();
		}
		return result;
	}


	private static String[] buildHTMLEntities() {
		if (null!=htmlEntities) {
			return htmlEntities;
		} else {
			String[] result = new String[64];
			
			result['<'] = "&lt;";
			result['>'] = "&gt;";		
			result['&'] = "&amp;";
			result['"'] = "&quot;";		
			result['\''] = "&apos;";
					
			return result;
		}
	}

	
	
	private final static long[] tensDivisor = buildTensDivisorArray();
	
    private static long[] buildTensDivisorArray() {
    	long tens = 1000000000000000000L;
    	int c = 0;
    	long[] result = new long[20];
    	while (tens>0) {
    		//System.out.println("pos "+c+" has "+tens);
    		
    		result[c++] = tens;
    		tens = tens/10;
    	}
    	assert(c==result.length-1) : "found "+c+" expected "+result.length;
   
    	return result;
    }
    
    private final static char[] onesChars = buildChars(1000, 1);
    private final static char[] tensChars = buildChars(1000, 10);
    private final static char[] thousChars = buildChars(1000, 100);

    
    private static char[] buildChars(int total, int run) {
    	char[] result = new char[total];
    	int c = 0;
    	int v = 0;
    	while (c<total) {
    		char value = (char)('0' + (v++));
    		int x = run;
    		while (--x>=0) {
    			result[c++] = value;
    		}
    		if ('9' == value) {
    			v=0;
    		}
    	}    	
    	return result;
    }
    

	public static int longToChars(long value, boolean useNegPara, final byte[] localBuffer, final int mask,
			int activePos) {
		boolean isNegative = value<0;
		if (isNegative) {
			if (useNegPara) {
				localBuffer[mask & activePos++] = (byte) '(';
			}
			localBuffer[mask & activePos++] = (byte) '-';
		    value = -value;
		}
		
		long nextValue = value;//at this point the value is absolute
		int orAll = 0; 
		int t = value <=  Integer.MAX_VALUE ? (value<=10000? 14: 8): 2;//skip high end if smaller value

		activePos = collectDigitChars(localBuffer, mask, activePos, nextValue, orAll, t);
		
		
		if (isNegative && useNegPara) {
			localBuffer[mask & activePos++] = (byte) ')';
		}
		return activePos;
	}
	

	private static int collectDigitChars(final byte[] localBuffer, final int mask,
											int activePos, long nextValue,
											int orAll, int t) {
		long tens;
		while (t!=tensDivisor.length && (tens=tensDivisor[t++])>=1) {
			t+=2;
		    int digit  = (int)(nextValue/tens);
		    nextValue  = nextValue%tens;
		    
		    char c;
		    orAll |= ((c=thousChars[digit])-'0');//this is to remove the leading zeros
		    if (0!=orAll) {
		    	localBuffer[mask & activePos++] = (byte)c;
		    }

		    orAll |= ((c=tensChars[digit])-'0');//this is to remove the leading zeros
		    if (0!=orAll) {
		    	localBuffer[mask & activePos++] = (byte)c;
		    }

		    orAll |= ((c=onesChars[digit])-'0');//this is to remove the leading zeros
		    if (0!=orAll) {
		    	localBuffer[mask & activePos++] = (byte)c;
		    }
		    
		}
		localBuffer[mask & activePos++] = (byte)onesChars[(int)nextValue];
		return activePos;
	}
	
	//TODO: add nearestMemoryUnit  B, K, M, G, T, P
    
}
