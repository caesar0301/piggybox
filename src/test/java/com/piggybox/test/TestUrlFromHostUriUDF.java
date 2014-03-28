package com.piggybox.test;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.http.UrlFromHostUri;

public class TestUrlFromHostUriUDF {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Test
	public void testUrlFromHostUri() throws ExecException {
		UrlFromHostUri func = new UrlFromHostUri();
		
		Tuple input1 = tupleFactory.newTuple();
		input1.append("http://www.example.com"); // host
		input1.append("/home?user=you&id=1"); // uri
		
		String res = func.exec(input1);
		Assert.assertEquals(res, "http://www.example.com/home?user=you&id=1");
		
		Tuple input2 = tupleFactory.newTuple();
		input2.append("http://www.example.com"); // host
		input2.append("http://www.example.com/home"); // uri

		res = func.exec(input2);
		Assert.assertEquals(res, "http://www.example.com/home");
	}
}
