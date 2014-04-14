package com.piggybox.test;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.piggybox.omnilab.GetAPBuildingInfo;

public class TestApInfo {
	private static final String APNameFull = "MH-BYGTSG-4F-01";
	private static final String APNameBuilding = "BYGTSG";
	private static final String APNameInvalid = "BYGTSG-test";
	
	@Test
	public void testApBuilbingInfo() throws ExecException{
		GetAPBuildingInfo APBI = new GetAPBuildingInfo();
		Tuple result = APBI.call(APNameFull);
		//System.out.println(result);
		Assert.assertEquals((String) result.get(0), "包玉刚图书馆");
		Assert.assertEquals((String) result.get(1), "LibBldg");
		Assert.assertEquals((String) result.get(2), "PUB");
		result = APBI.call(null); //test null input
		Assert.assertEquals(result.get(0), null);
		
		GetAPBuildingInfo APBI2 = new GetAPBuildingInfo(false);
		Tuple result2 = APBI2.call(APNameBuilding);
		Assert.assertEquals((String) result2.get(0), "包玉刚图书馆");
		
		GetAPBuildingInfo APBI3 = new GetAPBuildingInfo();
		Tuple result3 = APBI3.call(APNameInvalid);
		Assert.assertEquals((String) result3.get(0), "包玉刚图书馆");
	}
}
