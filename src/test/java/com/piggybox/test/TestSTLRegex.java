package com.piggybox.test;

import java.io.FileNotFoundException;

import org.junit.Test;

import com.piggybox.loader.STLRegex;

public class TestSTLRegex {

	@Test
	public void testLoader() throws FileNotFoundException{
		STLRegex stl = new STLRegex(",(?!\\s+)");
		// unfinished yet
		String input = "47326450,16-8月 -12 10.01.16.899 上午,1677726047,22550,19372,460005811540513,weibo 1.7,1,741,3992,tp2.sinaimg.cn,,1862344788";
	}
}
