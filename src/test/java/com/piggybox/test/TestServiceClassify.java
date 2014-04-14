package com.piggybox.test;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.piggybox.http.ServiceCategoryClassify;

public class TestServiceClassify {

	@Test
	public void testClassification() throws IOException{
		String host = "web2.qq.com";
		ServiceCategoryClassify scc = new ServiceCategoryClassify();
		Tuple result = scc.call(host);
		Assert.assertEquals((String) result.get(0), "腾讯网页QQ2");
		Assert.assertEquals((String) result.get(1), "即时通讯");
		Assert.assertEquals((String) result.get(2), "924");
	}
}
