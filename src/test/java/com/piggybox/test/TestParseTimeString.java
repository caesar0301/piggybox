package com.piggybox.test;

import java.text.ParseException;

import junit.framework.Assert;

import org.junit.Test;

import com.piggybox.omnilab.ParseTimeString;

public class TestParseTimeString {

	@Test
	public void testParser() throws ParseException{
		String timestr1 = "16-8月 -12 11.44.10.922 上午";
		ParseTimeString pts = new ParseTimeString();
		ParseTimeString ptsISO = new ParseTimeString("iso");
		Assert.assertEquals("1345088650.922", pts.call(timestr1));
		Assert.assertEquals("2012-08-16T11.44.10.922", ptsISO.call(timestr1));
	}
}
