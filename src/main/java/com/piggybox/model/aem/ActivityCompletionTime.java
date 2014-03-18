package com.piggybox.model.aem;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Generate measures related to performance for each activity.
 * @author chenxm
 *
 */
public class ActivityCompletionTime extends EvalFunc<DataBag>{
	private DataBag outputBag = BagFactory.getInstance().newDefaultBag(); // output result
	private double portion = 1.0;
	private Double activityStart = null; // activity start time
	private Double activityEnd = null;
	private long activityVol = 0;
	
	public ActivityCompletionTime(){
		this(0.95);
	}
	
	/**
	 * Compute the overall completion time of whole activity.
	 * From the start of the first entity to the end of the last one.
	 * @param portion (Percentage) controls the number of entities involved to compute the completion time.
	 */
	public ActivityCompletionTime(double portion){
		this.portion = portion;
	}
	
	/**
	 * The variables MUST be reset for a new input tuple.
	 */
	private void cleanup(){ 
		outputBag.clear();
		activityStart = null;
		activityEnd = null;
		activityVol = 0;
	}
	
	@Override
	public DataBag exec(Tuple b) throws IOException {
		cleanup();
		DataBag entityBag = (DataBag) b.get(0);
		activityVol = entityBag.size();
		long actualNumber = Math.round(activityVol*portion);
		for ( Tuple t :  entityBag ){
			//System.out.println("**********" + actualNumber);
			if ( actualNumber <= 0 )
				break;
			actualNumber--;
			
			Double eStartTime = (Double) t.get(0);
			Double eEndTime = (Double) t.get(1);
			
			if ( eStartTime == null || eEndTime == null)
				throw new IOException("The input tuple can not be null: " + eStartTime + ", " + eEndTime);

			if ( activityStart == null){
				activityStart = eStartTime;
				activityEnd = eEndTime;
				continue;
			}
			if ( eStartTime < activityStart )
				activityStart = eStartTime;
			if ( eEndTime > activityEnd)
				activityEnd = eEndTime;
		}
		
		Tuple newT = TupleFactory.getInstance().newTuple();
		try{
			newT.append(activityEnd-activityStart);
		} catch (Exception e){
			newT.append(null);
		}
		outputBag.add(newT);
		return outputBag;
	}
}
