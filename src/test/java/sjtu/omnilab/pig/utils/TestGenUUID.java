package sjtu.omnilab.pig.utils;

import java.io.IOException;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import sjtu.omnilab.pig.uuid.GenUUID;
import sjtu.omnilab.pig.uuid.GenUUIDRand;

public class TestGenUUID {
	private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testGenUUID() throws IOException {
    	Tuple input = tupleFactory.newTuple();
    	input.append("123");
        GenUUID func = new GenUUID();
        String uuid = func.exec(input);
        Assert.assertNotNull(UUID.fromString(uuid));
    }
    
    @Test
    public void testGenUUIDRand() throws IOException {
    	Tuple input = tupleFactory.newTuple(); // empty tuple
        GenUUIDRand func = new GenUUIDRand();
        String uuid = func.exec(input);
        Assert.assertNotNull(UUID.fromString(uuid));
    }
}