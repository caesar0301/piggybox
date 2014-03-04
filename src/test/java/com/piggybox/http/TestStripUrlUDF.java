package com.piggybox.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.http.ExtractUrlHost;
import com.piggybox.http.ExtractUrlParams;
import com.piggybox.http.StripUrl;
import com.piggybox.http.StripUrlLeft;
import com.piggybox.http.StripUrlRight;

public class TestStripUrlUDF {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Test
	public void testStrpUrlLeft() throws ExecException {
		Tuple input = tupleFactory.newTuple();
		input.append("http://www.example.com/home?user=you&id=1");
		StripUrlLeft func = new StripUrlLeft();
		String res = func.exec(input);
		Assert.assertEquals(res, "www.example.com/home?user=you&id=1");
	}
	
	@Test
	public void testStrpUrlRight() throws ExecException {
		Tuple input = tupleFactory.newTuple();
		input.append("http://www.example.com/home?user=you&id=1");
		StripUrlRight func = new StripUrlRight();
		String res = func.exec(input);
		Assert.assertEquals(res, "http://www.example.com/home");
	}
	
	@Test
	public void testStrpUrl() throws ExecException {
		Tuple input = tupleFactory.newTuple();
		input.append("http://www.example.com/home?user=you&id=1");
		StripUrl func = new StripUrl();
		String res = func.exec(input);
		Assert.assertEquals(res, "www.example.com/home");
	}
	
	@Test
	public void testExtractUrlParams() throws IOException {
		Tuple input = tupleFactory.newTuple();
		input.append("http://www.example.com/home?user=you&id=1");
		ExtractUrlParams func = new ExtractUrlParams();
		Tuple res = func.exec(input);
		Map<String, String> params = new HashMap<String, String>();
		params.put("user", "you");
		params.put("id", "1");
		Assert.assertEquals(res.get(0), params);
	}
	
	@Test
	public void testExtractUrlHost() throws IOException {
		Tuple input = tupleFactory.newTuple();
		input.append("http://www.example-test.com:80/home");
		ExtractUrlHost func = new ExtractUrlHost();
		String res = func.exec(input);
		Assert.assertEquals(res, "www.example-test.com");
	}
}
