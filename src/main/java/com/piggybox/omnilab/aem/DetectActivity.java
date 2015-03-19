package com.piggybox.omnilab.aem;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.Period;

import com.google.common.net.InternetDomainName;
import com.piggybox.utils.tree.TreeNode;

/**
 * The pig UDF implementing the AID algorithm of Activity-Entity Model.
 * Input: a bag of tuples (HttpRequestStartTime, HttpRequestEndTime, URL, Referrer, ContentType ..)
 * Return: a bag of tuples, each tuple being appended by an activity ID (UUID);
 * 
 * @author chenxm
 *
 */
public class DetectActivity extends AccumulatorEvalFunc<DataBag>{
	private double readingTime;
	private AEM aemModel = null;
	private DataBag outputBag = null;
	public Log myLogger = this.getLogger();
	
	public DetectActivity(){
		this("2s");
	}
	
	public DetectActivity(String timeSpec){
		Period p = new Period("PT" + timeSpec.toUpperCase());
	    this.readingTime = p.toStandardSeconds().getSeconds();
		cleanup();
	}
	
	@Override
	public void accumulate(Tuple b) throws ExecException {
		cleanup();
		for ( Tuple t : (DataBag) b.get(0) ){
			Entity newEntity = new Entity(t);
			boolean okToDump = false;
			okToDump = aemModel.addEntityToModel(newEntity);
			int actCnt = aemModel.size();
			if (okToDump && actCnt > 0){
				dumpActivitiesToBag(0, actCnt-1); // leave the last activity to add new entities.
			}
			//this.reporter.progress(); // Disable to pass mvn test
		}
	}

	@Override
	public void cleanup() {
		this.outputBag = BagFactory.getInstance().newDefaultBag();
		this.aemModel = new AEM(readingTime, this.myLogger); // set to 2.5s
	}

	@Override
	public DataBag getValue() {
		dumpActivitiesToBag(0, aemModel.size()); // dump all activities.
		return this.outputBag;
	}
	
	/**
	 * Dump activities from startIndex, inclusive, to endIndex, exclusive to the outputBag.
	 * @param startIndex
	 * @param endIndex
	 */
	private void dumpActivitiesToBag(int startIndex, int endIndex){
		for ( Activity act : aemModel.getActivities(startIndex, endIndex)){
			for ( Entity entity : act.getAllEntities() ){
				Tuple newT = TupleFactory.getInstance().newTuple(entity.getTuple().getAll());
				newT.append(act.getID());
				this.outputBag.add(newT);
			}
		}
		aemModel.removeActivities(startIndex, endIndex);
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
				throw new RuntimeException(String.format("Expected first element of tuple to be a DOUBLE, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			if (inputTupleSchema.getField(1).type != DataType.DOUBLE){
				throw new RuntimeException(String.format("Expected first element of tuple to be a DOUBLE, but instead found %s",
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
			if (inputTupleSchema.getField(5).type != DataType.CHARARRAY){
				throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
	                                             DataType.findTypeName(inputTupleSchema.getField(0).type)));
			}
			Schema outputTupleSchema = inputTupleSchema.clone();
			outputTupleSchema.add(new Schema.FieldSchema("activity_id", DataType.CHARARRAY));
			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
	                                           outputTupleSchema,
	                                           DataType.BAG));
		}catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}