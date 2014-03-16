package com.piggybox.http;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * 
 * @author chenxm
 */
public class ServiceCategoryClassify extends SimpleEvalFunc<String> {
	
	public String call(String useragent) {
		return useragent;
	}
}
