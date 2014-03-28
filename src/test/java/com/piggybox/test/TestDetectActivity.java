package com.piggybox.test;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.piggybox.model.aem.DetectActivity;
import com.piggybox.utils.PigUtils;

public class TestDetectActivity {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	
	@Test
	public void testDetectActivity() throws IOException{
		Tuple input = tupleFactory.newTuple();
		input.append(prepareBag());
		DetectActivity func = new DetectActivity();
		List<Tuple> result = PigUtils.databagToList(func.exec(input));
		Assert.assertEquals(6, result.size());
	}
	
	private DataBag prepareBag(){
		Tuple t1 = prepareTuple(1.0, 1.1, "http://www.bar.com/1.html", null, "text/html", "100");
		Tuple t2 = prepareTuple(1.1, 1.2, "http://www.bar.com/a.png", "http://www.bar.com/1.html", "image/png", "101");
		Tuple t3 = prepareTuple(1.1, 1.2, "http://www.bar.com/b.png", "http://www.bar.com/1.html", "image/png", "102");
		Tuple t4 = prepareTuple(6.1, 6.2, "http://www.bar.com/2.html", null, "text/html", "103");
		Tuple t5 = prepareTuple(6.1, 6.2, "http://www.bar.com/c.png", "http://www.bar.com/2.html", "image/png", "104");
		Tuple t6 = prepareTuple(20.1, 20.2, "http://www.bar.com/x.html", null, "text/html", "200");
		DataBag dataBag = bagFactory.newDefaultBag();
		dataBag.add(t1);
		dataBag.add(t2);
		dataBag.add(t3);
		dataBag.add(t4);
		dataBag.add(t5);
		dataBag.add(t6);
		return dataBag;
	}
	
	private Tuple prepareTuple(Double start, Double end, String url, String referrer, String type, String id){
		Tuple tuple = tupleFactory.newTuple();
		tuple.append(start);
		tuple.append(end);
		tuple.append(url);
		tuple.append(referrer);
		tuple.append(type);
		tuple.append(id);
		return tuple;
	}
}
