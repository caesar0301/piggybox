package com.piggybox.test;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.http.MobileType;

public class TestMobileTypeUDF {
	private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testMobileTypeUDF() throws IOException {

        Tuple input = tupleFactory.newTuple();
        input.append("iPhone; iPhone OS 6.1.3; zh_CN");
        MobileType func = new MobileType();
        String output = func.exec(input);
        Assert.assertEquals(output, "iphone");
    }
}
