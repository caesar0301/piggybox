package com.piggybox.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.net.InternetDomainName;
import com.piggybox.utils.SimpleEvalFunc;

/**
 * Get the top-level domain from URL.
 * For example, given url "www.baidu.com", this UDF returns "baidu.com" as results.
 * @author chenxm
 *
 */
public class TopPrivateDomain  extends SimpleEvalFunc<String>{
	public String call(String url){
		return getTopPrivateDomain(url);
	}
	
	@SuppressWarnings("deprecation")
	private String getTopPrivateDomain(String url){
		String host = getHost(url);
		try {
			host = InternetDomainName.from(host).topPrivateDomain().name();
		} catch (Exception e) {}
		return host; // maybe null
	}
	
	private String getHost(String url){
		if (url != null ){
			Pattern pattern = Pattern.compile("^(?:\\w+:?//)?([^:\\/\\?&]+)", Pattern.CASE_INSENSITIVE);
		    Matcher matcher = pattern.matcher(url);
		    if ( matcher.find() ){
		    	return matcher.group(1);
		    }
		}
	    return url;
	}
}
