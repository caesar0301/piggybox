package com.piggybox.test;

import org.junit.Assert;
import org.junit.Test;

import com.piggybox.converter.DoubleToString;

public class TestConverter {
	private Double bigDecimal = 3.828442696938E9;

	@Test
	public void testDouble2String(){
		Assert.assertEquals("3828442696.938",
				new DoubleToString().call(this.bigDecimal, "%.3f"));
	}
}
