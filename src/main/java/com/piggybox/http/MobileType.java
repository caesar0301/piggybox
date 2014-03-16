package com.piggybox.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a mobile user agent string, detect and return a device/OS name.
 * @version alpha
 * @author chenxm
 */
public class MobileType extends SimpleEvalFunc<String> {
	private static Pattern mobilePattern;

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
	 * @param userAgentString
	 * @return
	 */
	private String getDeviceType(String userAgentString) {
		mobilePattern = Pattern.compile(UserAgentClassify.MOB_STRING, Pattern.CASE_INSENSITIVE);
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
