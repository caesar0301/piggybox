package sjtu.omnilab.pig.model.aem;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

class ActHelper {
	String userAgent;
	String aid;
	List<String> urls;
	List<String> methods;
	List<String> codes;
	
	
	public ActHelper(String ua, String aid){
		this.userAgent= ua;
		this.aid = aid;
		this.urls = new LinkedList<String>();
		this.methods = new LinkedList<String>();
		this.codes = new LinkedList<String>();
	}
	
	public void addRequst(String url, String method, String code){
		this.urls.add(url);
		this.methods.add(method);
		this.codes.add(code);
	}
	
	public boolean isEqual(ActHelper a){
		if ( (this.userAgent == a.userAgent) || (this.userAgent!=null && a.userAgent!=null && this.userAgent.equals(a.userAgent)))
			if ( this.urls.get(0).equals(a.urls.get(0)))
				return true;
		return false;
	}
	
	public int size(){
		return this.urls.size();
	}
}

/**
 * Label an acitivity as one of "start", "forward","backward", and "refresh".
 * @author chenxm
 *
 */
public class LabelActivity extends AccumulatorEvalFunc<DataBag>{
	private DataBag outputBag = null;
	private List<ActHelper> activities = null;
	
	public LabelActivity(){
		cleanup();
	}

	@Override
	public void accumulate(Tuple b) throws IOException {
		for ( Tuple t : (DataBag) b.get(0) ){
			boolean added = false;
			String ua = (String) t.get(0);
			String url = (String) t.get(1);
			String method = (String) t.get(2);
			String code = (String) t.get(3);
			String aid = (String) t.get(4);
			if ( activities.size() > 0){
				ActHelper lastAct = activities.get(activities.size()-1);
				if ( aid.equals(lastAct.aid)){
					lastAct.addRequst(url, method, code);
					added = true;
				}
			}
			if ( ! added ){
				ActHelper newAct = new ActHelper(ua, aid);
				newAct.addRequst(url, method, code);
				activities.add(newAct);
			}
		}
		System.out.println("***********"+activities.size());
		
		for ( int i = 0; i < activities.size(); i++){
			String lab = "forward";
			if ( i == 0){
				lab = "start";
			} else if ( i > 0 ){
				for ( int j = i-1; j >= 0; j--){
					if ( activities.get(i).isEqual(activities.get(j)) ){
						if ( i - j == 1 )
							lab = "refresh";
						else
							lab = "backward";
						break;
					}
				}
			}
			Tuple newT = TupleFactory.getInstance().newTuple();
			newT.append(activities.get(i).aid);
			newT.append(lab);
			newT.append(activities.get(i).size());		// for debugging
			newT.append(activities.get(i).codes.get(0)); // for debugging
			newT.append(activities.get(i).urls.get(0)); // for debugging
			this.outputBag.add(newT);
			reporter.progress();
		}
	}

	@Override
	public void cleanup() {
		this.outputBag = BagFactory.getInstance().newDefaultBag();
		this.activities = new LinkedList<ActHelper>();
	}

	@Override
	public DataBag getValue() {
		return outputBag;
	}

	/**
	 * The output schema of AEM UDF.
	 * Bag in bag out. But the output bag elements are appended by an activityID.
	 */
	@Override
	public Schema outputSchema(Schema input){
		try {
			Schema.FieldSchema inputFieldSchema = input.getField(0);
			if (inputFieldSchema.type != DataType.BAG){
				throw new RuntimeException("Expected a BAG as input");
			}
			Schema inputBagSchema = inputFieldSchema.schema;
			if (inputBagSchema.getField(0).type != DataType.TUPLE){
				throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
	                                             DataType.findTypeName(inputBagSchema.getField(0).type)));
			}
			Schema inputTupleSchema = inputBagSchema.getField(0).schema;
			if (inputTupleSchema.getField(0).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(1).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(2).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(3).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(4).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			Schema outputTupleSchema = new Schema();
			outputTupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY)); // activity ID
			outputTupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY)); // label
			outputTupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY)); // size, for debugging
			outputTupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY)); // url, for debugging
			outputTupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY)); // url, for debugging
			return new Schema(new Schema.FieldSchema(null,outputTupleSchema,DataType.BAG));
		}catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
