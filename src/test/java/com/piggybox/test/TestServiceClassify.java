package com.piggybox.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.piggybox.http.ServiceCategoryClassify;

public class TestServiceClassify {

	@Test
	public void testClassification() throws IOException{
		String host = "web2.qq.com";
		ServiceCategoryClassify scc = new ServiceCategoryClassify();
		String result = scc.call(host);
		Assert.assertEquals(result, "腾讯网页QQ2;即时通讯;924");
	}
}
