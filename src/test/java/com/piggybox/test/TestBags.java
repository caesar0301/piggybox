package com.piggybox.test;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.bags.MergeTuples;

public class TestBags {
	private DataBag oneItemBag;
	
	public TestBags(){
		Tuple t1 = createTuple(10);
		Tuple t2 = createTuple(11);
		oneItemBag = BagFactory.getInstance().newDefaultBag();
		oneItemBag.add(t1); oneItemBag.add(t2);
	}

	@Test
	public void testBags() throws ExecException{
		MergeTuples mergeTuples = new MergeTuples();
		Tuple ret = mergeTuples.call(oneItemBag);
		Assert.assertEquals(ret.get(0), 10);
		Assert.assertEquals(ret.get(1), 11);
	}
	
	private Tuple createTuple(Object v1){
		return TupleFactory.getInstance().newTuple(v1);
	}
	
	private Tuple createTuple(Object v1, Object v2){
		Tuple tuple = TupleFactory.getInstance().newTuple(v1);
		tuple.append(v2);
		return tuple;
	}
}
