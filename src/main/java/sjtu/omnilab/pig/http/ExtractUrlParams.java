package sjtu.omnilab.pig.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Extract parameters in URL and return a map of <name, value> pairs.
 * Input: a tuple of (String)
 * @author chenxm
 *
 */
public class ExtractUrlParams extends EvalFunc<Tuple>{
	private Pattern mainUrlPattern = Pattern.compile("^(?:\\w+://)?(?:[^\\?&]+)(\\?.*)?$", Pattern.CASE_INSENSITIVE);
	private Pattern urlParamPattern = Pattern.compile("([^?=&]+)=([^?&=]+)");
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if ( input == null || input.size() == 0 || input.get(0) == null ){
			return null;
		}
		try {
			String URL = (String)input.get(0);
			Matcher matcher = mainUrlPattern.matcher(URL);
			Tuple output = tupleFactory.newTuple();
			if ( matcher.find() ){
				String paramStr = matcher.group(1);
				Map<String, String> params = extractParams(paramStr);
				output.append(params);
			} else {
				output.append(URL);
			}
			return output;
		} catch (Exception e) {
			throw new ExecException("Unknown exception: " + e);
		}
	}
	
	private Map<String, String> extractParams(String urlParams){
		Map<String, String> paramMap = new HashMap<String, String>();
		if ( urlParams.length() != 0 ){
			Matcher matcher = urlParamPattern.matcher(urlParams);
			while(matcher.find()){
				String name = matcher.group(1);
				String value = matcher.group(2);
				paramMap.put(name, value);
			}
		}
		return paramMap;
	}
	
	public Schema outputSchema(Schema input) {
		// Check that we were passed one field
		if ( input.size() != 1 )
			throw new RuntimeException("Excpeted input (String), input need only one field");
		try{
			// Get the types for column and check it.
			// If they are wring, figure out what types were passed and give a good error message.
			if (input.getField(0).type != DataType.CHARARRAY){
				String msg = "Excepted input (String), received schema (";
				msg += DataType.findTypeName(input.getField(0).type);
				msg += ")";
				throw new RuntimeException(msg);
			}
		} catch ( Exception e) {
			throw new RuntimeException(e);
		}
		// Assemble output schema
		Schema output = new Schema();
		FieldSchema paramMap = new FieldSchema("urlparams", DataType.MAP);
		output.add(paramMap);
		return output;
	}
}
