package com.piggybox.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Receive a mobile User Agent string and return a device type string.
 * 
 * @author chenxm
 */
public class MobileType extends SimpleEvalFunc<String> {
	private static Pattern mobilePattern;
	public static final String MOB_STRING = "android|(bb\\d+|meego).+mobile|avantgo|bada\\/|blackberry|blazer|compal|docomo|dolfin|dolphin|elaine|fennec|hiptop|iemobile|(hpw|web)os|htc( touch)?|ip(hone|od|ad)|iris|j2me|kindle( fire)?|lge |maemo|midp|minimo|mmp|netfront|nokia|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|playstation|pocket|portalmmm|psp|series(4|6)0|symbian|silk-accelerated|skyfire|sonyericsson|treo|tablet|touch(pad)?|up\\.(browser|link)|vodafone|wap|webos|windows (ce|phone)|wireless|xda|xiino|zune";

	public String call(String useragent) {
		String ua = useragent.toLowerCase();
		String type = getDeviceType(ua);
		if (type != null) {
			return type.toLowerCase();
		} else
			return "unknown";
	}

	/**
	 * Get the keyword in regular pattern that indicate it's mobile device.
	 * 
	 * @param userAgentString
	 * @return
	 */
	private String getDeviceType(String userAgentString) {
		mobilePattern = Pattern.compile(MOB_STRING, Pattern.CASE_INSENSITIVE);
		Matcher matcher = mobilePattern.matcher(userAgentString);
		if (matcher.find()) {
			int start = matcher.start();
			int end = matcher.end();
			String matchedString = userAgentString.substring(start, end);
			return matchedString;
		}
		return null;
	}
}
