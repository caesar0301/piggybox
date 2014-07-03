package com.piggybox.omnilab.aem;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Given a bag of timing pairs (STime, ETIme), calculate the visit frequency from user reading times.
 * The input pairs should be kept in ascending order.
 * Return a bag with single tuple.
 * @author chenxm
 *
 */
public class VisitFrequency extends AccumulatorEvalFunc<DataBag> {
	private Double lastSTime = null;
	private Double lastETime = null;
	private long number = 0;
	private Double readingTimeMean = 0.0;
	private Double totalReadingTime = 0.0;
	private Double VF = 0.0;

	@Override
	public void accumulate(Tuple input) throws IOException {
		DataBag inputBag = (DataBag) input.get(0);
		if (inputBag == null)
			throw new IllegalArgumentException("Expected a bag, got null");

		for (Tuple tuple : inputBag) {
			Double stime = (Double) tuple.get(0);
			Double etime = (Double) tuple.get(1);
			if (stime == null || etime == null )
				continue;
			if (lastETime != null && stime >= lastETime){
				updateReadingTime(stime-lastETime);
				VF = 1.0 / readingTimeMean;
			}
			lastSTime = stime;
			lastETime = etime;
		}
	}
	
	private void updateReadingTime(Double rt){
		totalReadingTime += rt;
		number += 1;
		// Calculate expected reading time for whole observations
		// instead of moving average.
		readingTimeMean = totalReadingTime/number;
	}

	@Override
	public void cleanup() {
		lastSTime = null;
		lastETime = null;
		number = 0;
		readingTimeMean = 0.0;
		totalReadingTime = 0.0;
		VF = 0.0;
	}

	@Override
	public DataBag getValue() {
		DataBag output= BagFactory.getInstance().newDefaultBag();
		output.add(TupleFactory.getInstance().newTuple(VF));
		return output;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return input;
	}
}
