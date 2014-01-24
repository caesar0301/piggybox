package sjtu.omnilab.pig.http;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestTopPrivateDomain {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Test
	public void testTopPrivateDomain() throws IOException {
		TopPrivateDomain func = new TopPrivateDomain();
		
		Tuple input = tupleFactory.newTuple();
		input.append("http://www.bbc.co.uk/news/world-middle-east-25805209");
		String res = func.exec(input);
		Assert.assertEquals(res, "bbc.co.uk");
		
		input = tupleFactory.newTuple();
		input.append("http://101.101.101.101/news/world-middle-east-25805209");
		res = func.exec(input);
		Assert.assertEquals(res, "101.101.101.101");
	}
}
