package sjtu.omnilab.pig.model.aem;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

class pair{
	public double start;
	public double end;
}

/**
 * Generate measures related to performance for each activity.
 * @author chenxm
 *
 */
public class MeasureActivity extends EvalFunc<DataBag>{
	private DataBag outputBag; // output result
	private String activityAddress; // URL of this activity
	private String activityLabel; // refresh,stop,backward,...
	private String apName; // user location during this activity
	private double activityStart = -1; // activity start time
	private double activityEnd = -1;
	private long activitySize = 0;
	private long activityVol = 0; // number of entities
	private List<Double> flowSrcLat; // latencies
	private List<Double> flowDstLat;
	private List<Double> flowSrcJitter; // jitter
	private List<Double> flowDstJitter;
	private List<Double> entityDataRates; // data rates
	private Set<String> hostSet;
	
	public MeasureActivity(){
		this.init();
	}
	
	@Override
	public DataBag exec(Tuple b) throws IOException {
		init();
		for ( Tuple t : (DataBag) b.get(0) ){
			this.activityVol += 1;
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
			String reqUa = (String) t.get(42);
			String reqRef = (String) t.get(43);
			String rspCT = (String) t.get(46);
			Boolean itrr = (Boolean) t.get(50);
			String label = (String) t.get(51);
			
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
			if ( activityStart == -1)
				activityStart = reqTime;
			else if ( reqTime < activityStart )
				activityStart = reqTime;
			double rspend = reqTime + 0.01; // lower bounds
			if (rspTime != null && rspDur != null)
				rspend = rspTime+rspDur;
			if ( activityEnd < rspend)
				activityEnd = rspend;
			long tsize = 0;
			if (reqPl != null && rspPl !=  null )
				tsize = reqPl + rspPl;
			activitySize += tsize;
			if ( srcRttAvg != null )
				this.flowSrcLat.add(srcRttAvg);
			if ( dstRttAvg != null )
				this.flowDstLat.add(dstRttAvg);
			if ( srcRttStd != null )
				this.flowSrcJitter.add(srcRttStd);
			if ( dstRttStd != null )
				this.flowDstJitter.add(dstRttStd);
			double tdur = rspend - reqTime;
			if ( tdur > 0)
				this.entityDataRates.add(tsize/tdur);
		}
		
		return assembleResult();
	}

	public void init() {
		this.outputBag = BagFactory.getInstance().newDefaultBag();
		this.flowSrcLat = new LinkedList<Double>();
		this.flowDstLat = new LinkedList<Double>();
		this.flowSrcJitter = new LinkedList<Double>();
		this.flowDstJitter = new LinkedList<Double>();
		this.entityDataRates = new LinkedList<Double>();
		this.hostSet = new HashSet<String>();
		this.activityAddress = null;
		this.activityLabel = null;
		this.apName = null;
		this.activityStart=-1;
		this.activityEnd=-1;
		this.activitySize=0;
		this.activityVol=0;
	}

	public DataBag assembleResult() {
		Tuple newT = TupleFactory.getInstance().newTuple();
		newT.append(activityStart); //start time
		double dur = activityEnd-activityStart;
		newT.append(activityVol); // entity count
		newT.append(activitySize); // size
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
		if ( uri.matches("^(\\w+:?//).*")){
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
