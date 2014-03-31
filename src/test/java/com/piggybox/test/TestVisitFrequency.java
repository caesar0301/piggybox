package com.piggybox.test;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.model.aem.VisitFrequency;

public class TestVisitFrequency {

	@Test
	public void testVF() throws IOException{
		DataBag input = BagFactory.getInstance().newDefaultBag();
		input.add(createPair(0, 1));
		input.add(createPair(2, 2.6));
		input.add(createPair(3, 4));
		input.add(createPair(5, 6));
		VisitFrequency vf = new VisitFrequency();
		DataBag result = vf.exec(TupleFactory.getInstance().newTuple(input));
		for (Tuple t: result){
			Assert.assertEquals(t.get(0), 1.25);
		}
		
	}
	
	private Tuple createPair(double v1, double v2){
		Tuple pair = TupleFactory.getInstance().newTuple(v1);
		pair.append(v2);
		return pair;
	}
}
