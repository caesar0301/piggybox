package com.piggybox.test;

import java.io.IOException;

import com.piggybox.bags.*;
import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BagToTuple;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestBags {
	private DataBag oneItemBag;
	private DataBag twoItemBag;
	
	public TestBags(){
		// input: {(10), (11)}
		Tuple t1 = createTuple(10);
		Tuple t2 = createTuple(11);
		oneItemBag = BagFactory.getInstance().newDefaultBag();
		oneItemBag.add(t1);
		oneItemBag.add(t2);

		Tuple t3 = createTuple("A"); t3.append(5);
		Tuple t4 = createTuple("A"); t4.append(6);
		Tuple t5 = createTuple("B"); t5.append(7);
		Tuple t6 = createTuple("C"); t6.append(8);
		twoItemBag = BagFactory.getInstance().newDefaultBag();
		twoItemBag.add(t3);
		twoItemBag.add(t4);
		twoItemBag.add(t5);
		twoItemBag.add(t6);
	}

	@Test
	public void testMergeTuples() throws ExecException{
		MergeTuples mergeTuples = new MergeTuples();
		Tuple ret = mergeTuples.call(oneItemBag);
		Assert.assertEquals(ret.get(0), 10);
		Assert.assertEquals(ret.get(1), 11);
	}
	
	@Test
	public void testNthTupleFromBag() throws IOException{
		NthTupleFromBag ntfb = new NthTupleFromBag();
		Tuple ret = ntfb.call(1, oneItemBag, null);
		Assert.assertEquals(ret.get(0), 11);
	}
	
	private Tuple createTuple(Object v1){
		return TupleFactory.getInstance().newTuple(v1);
	}
	
	private Tuple createTuple(Object v1, Object v2){
		Tuple tuple = TupleFactory.getInstance().newTuple(v1);
		tuple.append(v2);
		return tuple;
	}
	
	@Test
	public void testJoinBy() throws ExecException{
		DataBag input = BagFactory.getInstance().newDefaultBag();
		Tuple inputTuple = TupleFactory.getInstance().newTuple();
		inputTuple.append("hello");
		inputTuple.append("world");
		input.add(inputTuple);
		JoinEachBy joinBy = new JoinEachBy();
		DataBag output = joinBy.call(input, ";");
		for (Tuple t : output ){
			Assert.assertEquals(t.get(0), "hello;world");
		}
	}

	@Test
	public void TestCountEachBy() throws ExecException {
		CountEachBy ceb = new CountEachBy();
		DataBag result = ceb.call(twoItemBag, 0);
		for ( Tuple t : result ) {
			if ( t.get(0).equals("A") )
				Assert.assertEquals(t.get(1), new Long(2));
			if ( t.get(0).equals("B") )
				Assert.assertEquals(t.get(1), new Long(1));
		}
	}

	@Test
	public void TestSumEachBy() throws ExecException {
		SumEachBy ceb = new SumEachBy();
		DataBag result = ceb.call(twoItemBag, 0, 1);
		for ( Tuple t : result ) {
			if ( t.get(0).equals("A") )
				Assert.assertEquals(t.get(1), new Double(11));
			if ( t.get(0).equals("B") )
				Assert.assertEquals(t.get(1), new Double(7));
		}
	}

}
