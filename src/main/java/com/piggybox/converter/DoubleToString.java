package com.piggybox.converter;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Format a double number into plain string, with scientific notation removed if it is present.
 * 
 * <p>
 * The function DoubleToString(d, format) takes two parameters and the format is
 * to control string appearance. The format accommodates types in String.format() in java.
 * </p>
 * @author chenxm
 *
 */
public class DoubleToString extends SimpleEvalFunc<Double>{
	
	public String call(Double d, String format){
		if (d == null)
			return null;
		return String.format(format, d.doubleValue());
	}
}
