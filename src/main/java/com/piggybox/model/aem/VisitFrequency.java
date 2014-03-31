package com.piggybox.model.aem;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Given a bag of timing pairs (STime, ETIme) , calculate the visit frequency from user reading times.
 * Return a bag with single tuple.
 * @author chenxm
 *
 */
public class VisitFrequency extends AccumulatorEvalFunc<DataBag> {
	private Double lastSTime = null;
	private Double lastETime = null;
	private long totalPairs = 0;
	private Double readingTimeEx = 0.0;
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
			if (lastSTime != null ){
				updateReadingTime(stime-lastETime);
				VF = 1.0 / readingTimeEx;
			}
			lastSTime = stime;
			lastETime = etime;
			totalPairs++;
		}
	}
	
	private void updateReadingTime(Double rt){
		totalReadingTime += rt;
		readingTimeEx = 1.0*(readingTimeEx*(totalPairs-1) + rt)/(totalPairs);
	}

	@Override
	public void cleanup() {
		lastSTime = null;
		lastETime = null;
		totalPairs = 0;
		readingTimeEx = 0.0;
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
