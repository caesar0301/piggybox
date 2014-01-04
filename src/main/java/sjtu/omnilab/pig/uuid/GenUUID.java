package sjtu.omnilab.pig.uuid;

import java.io.IOException;
import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


/**
 * A wrapper to generate UUID from a string seed.
 * @author chenxm
 *
 */
public class GenUUID extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null )
			throw new IOException("Excepted input (String .. )");
		try {
			String str = (String)input.get(0);
			return UUID. nameUUIDFromBytes(str.getBytes()).toString();
		} catch (Exception e) {
			throw new IOException("Unknown exception ", e);
		}
	}
}
