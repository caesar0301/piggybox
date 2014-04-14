package com.piggybox.test;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.omnilab.aem.PerceivedCompletionTime;

public class TestActivityCompletionTime {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	
	@Test
	public void testActivityCompletionTime() throws IOException{
		DataBag input = prepareInput();
		PerceivedCompletionTime func = new PerceivedCompletionTime(0.7);
		Double output = func.call(input);
		Assert.assertEquals(1.5, output.doubleValue());
	}
	
	private DataBag prepareInput(){
		DataBag dataBag = bagFactory.newDefaultBag();
		Tuple t1 = prepareInputItem(1.0, 2.0);
		Tuple t2 = prepareInputItem(1.5, 2.5);
		Tuple t3 = prepareInputItem(2.0, 3.0);
		dataBag.add(t1);
		dataBag.add(t2);
		dataBag.add(t3);
		return dataBag;
	}
	
	private Tuple prepareInputItem(Double start, Double end){
		Tuple tuple = tupleFactory.newTuple();
		tuple.append(start);
		tuple.append(end);
		return tuple;
	}
}
