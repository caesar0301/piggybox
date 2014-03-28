package com.piggybox.model.aem;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a bag of (STime, ETime) pairs, the perceived duration is calculated.
 * I.e., D = max(ETime)-min{STime}.
 * This routine is involved in AEM model to calculate the activity completion time and session length.
 * @author chenxm
 *
 */
public class PerceivedCompletionTime extends SimpleEvalFunc<Double>{
	private double portion = 1.0;
	private Double activityStart = null; // activity start time
	private Double activityEnd = null;
	private long activityVol = 0;
	
	public PerceivedCompletionTime(){
		this(0.95);
	}
	
	/**
	 * Compute the overall completion time of whole activity.
	 * From the start of the first entity to the end of the last one.
	 * @param portion (Percentage) controls the number of entities involved to compute the completion time.
	 */
	public PerceivedCompletionTime(double portion){
		this.portion = portion;
	}
	
	/**
	 * The variables MUST be reset for a new input tuple.
	 */
	private void cleanup(){ 
		activityStart = null;
		activityEnd = null;
		activityVol = 0;
	}
	
	public Double call(DataBag b) throws IOException {
		cleanup();
		Double result;
		DataBag entityBag = b;
		activityVol = entityBag.size();
		long actualNumber = Math.round(activityVol*portion);
		for ( Tuple t :  entityBag ){
			//System.out.println("**********" + actualNumber);
			if ( actualNumber <= 0 )
				break;
			actualNumber--;
			
			Double eStartTime = (Double) t.get(0);
			Double eEndTime = (Double) t.get(1);
			
			if ( eStartTime == null || eEndTime == null){
				continue; // Skip invalid tuples
			}

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
		
		try{
			result = activityEnd-activityStart;
		} catch (Exception e){
			result = null;
		}
		return result;
	}
}
