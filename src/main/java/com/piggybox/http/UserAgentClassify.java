 package com.piggybox.http;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a user agent string, this UDF classifies clients to 'mobile' and
 * 'desktop'. Similar to datafu.pig.urls.UserAgentClassify, this function takes
 * another version of regex.
 * @version beta
 * @author chenxm
 */
public class UserAgentClassify extends SimpleEvalFunc<String> {
	public static final String MOB_STRING = 
			"android|(bb\\d+|meego).+mobile|avantgo|bada\\/|blackberry|blazer|compal|docomo|dolfin|dolphin|elaine|fennec|hiptop|iemobile|(hpw|web)os|htc( touch)?|ip(hone|od|ad)|iris|j2me|kindle( fire)?|lge |maemo|midp|minimo|mmp|netfront|nokia|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|playstation|pocket|portalmmm|psp|series(4|6)0|symbian|silk-accelerated|skyfire|sonyericsson|treo|tablet|touch(pad)?|up\\.(browser|link)|vodafone|wap|webos|windows (ce|phone)|wireless|xda|xiino|zune";

	public String call(String useragent) {
		String ua = useragent.toLowerCase();
		if (ua.matches(UserAgentClassify.MOB_STRING))
			return "0";
		else
			return "1";
	}
}
