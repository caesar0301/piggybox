package sjtu.omnilab.pig.model.aem;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

class ActHelper {
	String aid;
	List<String> urls;	
	
	public ActHelper(String aid, String url){
		this.aid = aid;
		this.urls = new LinkedList<String>();
		this.urls.add(url);
	}
	
	public void addRequst(String url){
		this.urls.add(url);
	}
	
	public boolean isEqual(ActHelper a){
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
public class LabelActivity extends EvalFunc<DataBag>{
	private DataBag outputBag;
	private Set<String> aidSet;
	private List<ActHelper> activities;
	
	public LabelActivity(){
		init();
	}
	
	public void init() {
		this.outputBag = BagFactory.getInstance().newDefaultBag();
		this.activities = new LinkedList<ActHelper>();
		this.aidSet = new HashSet<String>();
	}
	
	@Override
	public DataBag exec(Tuple b) throws IOException {
		init();
		for ( Tuple t : (DataBag) b.get(0) ){
			String url = (String) t.get(1);
			String aid = (String) t.get(2);
			addHttpEntity(aid, url);
		}
		System.out.println("***********"+activities.size());
		labelAct();
		return this.outputBag;
	}
	
	private void addHttpEntity(String aid, String url){
		this.aidSet.add(aid);
		boolean flag = false;
		for ( ActHelper act : this.activities ){
			if ( aid.equals(act.aid) ){
				act.addRequst(url);
				flag = true;
				break;
			}
		}
		if ( !flag ){
			ActHelper newAct = new ActHelper(aid, url);
			this.activities.add(newAct);
		}
	}
	
	private void labelAct(){
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
			newT.append(activities.get(i).urls.get(0)); // for debugging
			this.outputBag.add(newT);
			reporter.progress();
		}
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
			if (inputTupleSchema.getField(0).type != DataType.DOUBLE){
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
