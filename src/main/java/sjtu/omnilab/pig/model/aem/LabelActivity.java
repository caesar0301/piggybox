package sjtu.omnilab.pig.model.aem;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
	
	public String assembleUrls(){
		String res = "";
		for (String url : this.urls){
			res += url;
			res += ";";
		}
		return res;
	}
}

/**
 * Label an acitivity as one of "start", "forward","backward", and "refresh".
 * @author chenxm
 *
 */
public class LabelActivity extends EvalFunc<DataBag>{
	private DataBag outputBag;
	private Map<String, Integer> aidIndexMap;
	private List<ActHelper> activities;
	private long total = 0;
	
	public LabelActivity(){
		init();
	}
	
	public void init() {
		this.outputBag = BagFactory.getInstance().newDefaultBag();
		this.activities = new LinkedList<ActHelper>();
		this.aidIndexMap = new HashMap<String, Integer>();
		this.total = 0;
	}
	
	@Override
	public DataBag exec(Tuple b) throws IOException {
		init();
		for ( Tuple t : (DataBag) b.get(0) ){
			this.total += 1;
			String url = (String) t.get(1);
			String aid = (String) t.get(2);
			if ( url == null || aid == null)
				continue;
			addHttpEntity(aid, url);
			int size = this.activities.size();
			int left = 300;
			if ( size >= 2000 ){
				labelAct(left);
				this.activities.subList(0, size-left).clear();
				updateIndexMap();
			}
		}
		labelAct(0);
		System.out.println("***********"+total);
		return this.outputBag;
	}
	
	private void addHttpEntity(String aid, String url){
		if ( this.aidIndexMap.containsKey(aid) ){
			int index = this.aidIndexMap.get(aid);
			ActHelper act = activities.get(index);
			act.addRequst(url);
		} else {
			ActHelper newAct = new ActHelper(aid, url);
			this.aidIndexMap.put(aid, this.activities.size());
			this.activities.add(newAct);
		}
	}
	
	private void labelAct(int left){
		for ( int i = 0; i < activities.size()-left; i++){
			String lab = "forward";
			if ( i == 0){
				lab = "start";
			} else if ( i > 0 ){
				for ( int j = i-1; j >= 0 && j >= i-200; j--){ // past 200 activities
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
//			newT.append(activities.get(i).size());		// for debugging
//			newT.append(activities.get(i).assembleUrls()); // for debugging
			this.outputBag.add(newT);
			reporter.progress("LabelActivity is running: " + i + "th activity.");
		}
	}
	
	private void updateIndexMap(){
		this.aidIndexMap.clear();
		for( int index =0; index < this.activities.size(); index++){
			String aid = this.activities.get(index).aid;
			this.aidIndexMap.put(aid, index);
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
