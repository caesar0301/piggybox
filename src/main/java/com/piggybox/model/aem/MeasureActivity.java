package com.piggybox.model.aem;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Generate measures related to performance for each activity.
 * @author chenxm
 */
public class MeasureActivity extends EvalFunc<DataBag>{
	private DataBag outputBag; // output result
	private String activityAddress = null; // URL of this activity
	private String activityLabel = null; // refresh,stop,backward,...
	private String apName = null; // user location during this activity
	private Double activityStart = null; // activity start time
	private Double activityEnd = null;
	private long activitySize = 0;
	private long activityVol = 0; // number of entities
	private List<Double> flowSrcLat = new LinkedList<Double>(); // latencies
	private List<Double> flowDstLat = new LinkedList<Double>();
	private List<Double> flowSrcJitter = new LinkedList<Double>(); // jitter
	private List<Double> flowDstJitter = new LinkedList<Double>();
	private List<Double> entityDataRates = new LinkedList<Double>(); // data rates
	private Set<String> hostSet = new HashSet<String>();
	private double portion = 1.0;
	private Log logger = this.getLogger();
	
	public MeasureActivity(){
		this(0.95);
	}
	
	public MeasureActivity(double portion){
		outputBag = BagFactory.getInstance().newDefaultBag();
		this.portion = portion;
	}
	
	/**
	 * The variables MUST be reset for a new input tuple.
	 */
	private void cleanup() {
		outputBag.clear();
		activityAddress = null;
		activityLabel = null;
		apName = null;
		activityStart = null;
		activityEnd = null;
		activitySize = 0;
		activityVol = 0;
		flowSrcLat.clear();
		flowDstLat.clear();
		flowSrcJitter.clear();
		flowDstJitter.clear();
		entityDataRates.clear();
		hostSet.clear();
	}
	
	@Override
	public DataBag exec(Tuple b) throws IOException {
		cleanup();
		DataBag entityBag = (DataBag) b.get(0);
		logger.debug("***** Activity original volume: " + entityBag.size());
		activityVol = (int) Math.round(entityBag.size()*portion);
		long counter = activityVol;
		for ( Tuple t :  entityBag ){
			if ( counter <= 0 )
				break;
			counter--;
			String ap = (String) t.get(1);
			Double srcRttAvg = (Double) t.get(13);
			Double dstRttAvg = (Double) t.get(14);
			Double srcRttStd = (Double) t.get(15);
			Double dstRttStd = (Double) t.get(16);
			Double reqTime = (Double) t.get(30);
			Double rspTime = (Double) t.get(33);
			Double rspDur = (Double) t.get(34);
			Long reqPl = (Long) t.get(36);
			Long rspPl = (Long) t.get(37);
			String reqUrl = (String) t.get(39);
			String reqHost = (String) t.get(41);
			String reqRef = (String) t.get(43);
			String rspCT = (String) t.get(46);
			Boolean itrr = (Boolean) t.get(50);
			String label = (String) t.get(51);
			
			if ( reqTime == null || rspTime == null || rspDur == null ){
				String msg = "*****Please make sure the fileds [reqTime, rspTime, rspDur] have no null value.";
				logger.error(msg);
				throw new IOException(msg);
			}
			
			if ( reqHost != null )
				hostSet.add(reqHost);
			if ( activityAddress == null){
				if ( hasProtoPrefix(reqUrl))
					activityAddress = reqUrl;
				else
					activityAddress = reqHost+reqUrl;
				if ( rspCT != null && reqRef != null && !rspCT.contains("text"))
					activityAddress = reqRef;
			}
			if ( itrr != null && itrr == true){
				activityLabel = "itrr";
			} else {
				activityLabel = label;
			}
			if ( apName == null)
				apName = ap;
			
			if ( activityStart == null){
				activityStart = reqTime;
				activityEnd = rspTime+rspDur;
			} else {
				if ( reqTime < activityStart )
					activityStart = reqTime;
				if ( rspTime+rspDur > activityEnd )
					activityEnd = rspTime+rspDur;
			}
			// Size
			long tsize = 0;
			if (reqPl != null && rspPl !=  null )
				tsize = reqPl + rspPl;
			activitySize += tsize;
			double entityDuration = rspTime-reqTime+rspDur;
			if ( entityDuration > 0)
				entityDataRates.add(tsize/entityDuration);
			// Time
			if ( srcRttAvg != null )
				this.flowSrcLat.add(srcRttAvg);
			if ( dstRttAvg != null )
				this.flowDstLat.add(dstRttAvg);
			if ( srcRttStd != null )
				this.flowSrcJitter.add(srcRttStd);
			if ( dstRttStd != null )
				this.flowDstJitter.add(dstRttStd);
		}
		// Prepare output
		Tuple newT = TupleFactory.getInstance().newTuple();
		newT.append(activityStart); //start time
		newT.append(activityVol); // entity count
		newT.append(activitySize); // size
		double dur = activityEnd-activityStart;
		newT.append(dur); // duration
		double dr = 0;
		if ( dur > 0 )
			dr = activitySize/dur;
		newT.append(dr); // activitiy data rate
		newT.append(apName);
		newT.append(activityLabel);
		newT.append(mean(flowSrcLat)); // latency
		newT.append(mean(flowDstLat));
		newT.append(mean(flowSrcJitter)); //jitter
		newT.append(mean(flowDstJitter));
		newT.append(mean(entityDataRates)); // entitiy data rate
		newT.append(catStrings(hostSet, ";"));
		newT.append(activityAddress);
		this.outputBag.add(newT);
		return outputBag;
	}

	private double mean(List<Double> vals){
		double tot = 0;
		int n = 0;
		for ( Double val : vals){
			tot += val;
			n += 1;
		}
		return tot/n;
	}
	
	private boolean hasProtoPrefix(String uri){
		if ( uri != null && uri.matches("^(\\w+:?//).*")){
			return true;
		}
		return false;
	}
	
	private String catStrings(Collection<String> vals, String sep){
		String res = "";
		for ( String val : vals){
			if ( res.length() == 0)
				res += val;
			else {
				res += sep;
				res += val;
			}
		}
		return res;
	}
}
