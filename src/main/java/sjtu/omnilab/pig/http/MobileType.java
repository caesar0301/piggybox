package sjtu.omnilab.pig.http;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


/**
 * Receive a mobile User Agent string and return a device type string.
 * @author chenxm
 */
public class MobileType extends EvalFunc<Tuple> {
	private static Pattern mobilePattern;
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	public static final String MOB_STRING = "android|(bb\\d+|meego).+mobile|avantgo|bada\\/|blackberry|blazer|compal|docomo|dolfin|dolphin|elaine|fennec|hiptop|iemobile|(hpw|web)os|htc( touch)?|ip(hone|od|ad)|iris|j2me|kindle( fire)?|lge |maemo|midp|minimo|mmp|netfront|nokia|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|playstation|pocket|portalmmm|psp|series(4|6)0|symbian|silk-accelerated|skyfire|sonyericsson|treo|tablet|touch(pad)?|up\\.(browser|link)|vodafone|wap|webos|windows (ce|phone)|wireless|xda|xiino|zune";
	
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null )
			return null;
		try {
			progress(); // heart beat
			String str = (String)input.get(0);
			String type = getDeviceType(str);
			if (type != null ){
				return tupleFactory.newTuple(type.toLowerCase());
			}
			return tupleFactory.newTuple("unknown");
		} catch (Exception e) {
			throw new IOException("Caught exception ", e);
		}
	}
	
	/**
	 * Perform input schema checking and output schema construction.
	 */
	public Schema outputSchema(Schema input){
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
		return new Schema(new FieldSchema("type", DataType.CHARARRAY));
	}
	
	/**
	 * Get the keyword in regular pattern that indicate it's mobile device.
	 * @param userAgentString
	 * @return
	 */
	private String getDeviceType(String userAgentString){
		mobilePattern = Pattern.compile(MOB_STRING, Pattern.CASE_INSENSITIVE);
		Matcher matcher = mobilePattern.matcher(userAgentString);
		if ( matcher.find() ){
			int start = matcher.start();
			int end = matcher.end();
			String matchedString = userAgentString.substring(start, end);
			return matchedString;
		}
		return null;
	}
}
