package com.piggybox.omnilab.hzmob;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.data.Tuple;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Convert unformated time stamps in Hangzhou dataset into ISO8601 and UTC format.
 * <p>
 * For example:
 * <pre>
 * {@code
 * DEFINE parseTimeString com.piggybox.omnilab.hzmob.parseTimeString();
 * DEFINE parseTimeStringISO com.piggybox.omnilab.hzmob.parseTimeString("iso");
 * 
 * -- input: 
 * -- (7-AUG-12 11.05.07.418 PM)
 * input = LOAD 'input' AS (timestr:CHARARRAY);
 * 
 * -- output1: 
 * -- (1344351907.418)
 * -- output2: 
 * -- (2012-08-07T23.05.07.418)
 * output = FOREACH input GENERATE parseTimeString(timestr);
 * output2 = FOREACH input GENERATE parseTimeStringISO(timestr);
 * }
 * </pre>
 * </p>
 * @author chenxm
 */
public class ParseTimeString extends SimpleEvalFunc<Tuple>{
	private boolean needISO = false;
	
	public ParseTimeString(){}
	
	/**
	 * Constructor with format trigger.
	 * @param opt
	 */
	public ParseTimeString(String opt){
		if (opt.equals("iso"))
			this.needISO = true;
	}

	public String call(String timestr) throws ParseException{
		/* Timestamps in HZ mobile data, e.g., 
		 * 16-8月 -12 11.44.10.922 上午
		 * 17-AUG-12 01.29.07.727 PM	
		 */
		if (this.needISO)
			return this.parseTimeISO(timestr);
		else
			return this.parseTimeUTC(timestr);
	}
	
	/**
	 * Time format represented by seconds from UNIX epoch.
	 * @param timestr
	 * @return
	 * @throws ParseException 
	 */
	private String parseTimeUTC(String timestr) throws ParseException{
		String timeISO = this.parseTimeISO(timestr);
		if (timeISO == null)
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+08"));
		return String.format("%.3f", sdf.parse(timeISO).getTime()/1000.0);
	}
	
	/**
	 * The time format similar to ISO8601
	 * @param timestr
	 * @return
	 */
	private String parseTimeISO(String timestr){
		if (timestr == null)
			return null;

		final Map<String, Integer> monthMap = new HashMap<String, Integer>();
		monthMap.put("JAN", 1);
		monthMap.put("FEB", 2);
		monthMap.put("MAR", 3);
		monthMap.put("APR", 4);
		monthMap.put("MAY", 5);
		monthMap.put("JUNE", 6);
		monthMap.put("JULY", 7);
		monthMap.put("AUG", 8);
		monthMap.put("SEPT", 9);
		monthMap.put("OCT", 10);
		monthMap.put("NOV", 11);
		monthMap.put("DEC", 12);

		Pattern p = Pattern.compile("(\\d{1,2})-([^-]*)-(\\d{2,4})\\s((?:\\d+\\.){3}\\d+\\s+[^\\s]*)",
				Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(timestr);
		if ( m.find() ){
			int day = Integer.parseInt(m.group(1));
			// Parse month
			String rawmonth = m.group(2);
			Matcher monthMatcher = Pattern.compile("\\d+").matcher(rawmonth);
			int month = -1;
			if (monthMatcher.find()) // Match the number N out of "N月*"
				month = Integer.parseInt(monthMatcher.group());
			else if ( monthMap.containsKey(rawmonth) )
				month = monthMap.get(rawmonth);
			else
				return null;
			// Parse year
			int year = Integer.parseInt("20" + m.group(3));
			String time24 = parseTimeHMS24(m.group(4));
			return String.format("%4d-%02d-%02dT%s", year, month, day, time24);
		}
		return null;
	}
	
	private String parseTimeHMS24(String tstr){
		if ( tstr == null )
			return null;
		tstr = tstr.trim();
		String[] parts = tstr.split("\\s+", 2);
		if (parts.length != 2)
			return null;
		String timeStr = parts[0];
		int apm = 1;
		if (parts[1].equals("AM") || parts[1].equals("上午"))
			apm = 0;
		parts = timeStr.split("\\.", 2);
		if ( parts.length != 2)
			return null;
		int hour = Integer.parseInt(parts[0]);
		String minute = parts[1];
		if (apm == 0){
			if (hour >= 0 && hour <= 12)
				hour %= 12;
		} else {
			if (hour >= 1 && hour <= 12)
				hour = 12 + hour % 12;
		}
		return String.format("%02d.%s", hour, minute);
	}
}
