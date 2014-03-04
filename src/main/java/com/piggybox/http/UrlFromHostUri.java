package com.piggybox.http;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


/**
 * Concatenate Host and Uri fileds in HTTP header into a URL address.
 * Input: a tuple of two elements (Host, URI)
 * @author chenxm
 *
 */
public class UrlFromHostUri extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws ExecException {
		if ( input == null || input.size() < 2 || input.get(0) == null || input.get(1) == null ){
			return null;
		}
		try {
			String host = (String)input.get(0);
			String uri = (String)input.get(1);
			if ( hasProtoPrefix(uri) ){
				return uri;
			} else {
				return host+uri;
			}
		} catch (Exception e) {
			throw new ExecException("Unknown exception: " + e);
		}
	}
	
	private boolean hasProtoPrefix(String uri){
		if ( uri.matches("^(\\w+:?//).*")){
			return true;
		}
		return false;
	}

}
