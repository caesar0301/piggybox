package sjtu.omnilab.pig.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Strip the protocol prefix in URL address and return the left part.
 * @author chenxm
 *
 */
public class StripUrlLeft extends EvalFunc<String>{
	private Pattern pattern = Pattern.compile("^(\\w+:?//)?(.*)$", Pattern.CASE_INSENSITIVE);
	
	@Override
	public String exec(Tuple input) throws ExecException {
		if ( input == null || input.size() == 0 || input.get(0) == null ){
			return null;
		}
		try {
			String URL = (String)input.get(0);
			Matcher matcher = pattern.matcher(URL);
			if ( matcher.find() ){
				return matcher.group(2);
			} else {
				return URL;
			}
		} catch (Exception e) {
			throw new ExecException("Unknown exception: " + e);
		}
	}
	
}
