package com.piggybox.bags;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Merge all tuples in a bag into an single new tuple.
 * <p>
 * Example
 * <pre>
 * {@code
 * DEFINE MergeTuples com.piggybox.bags.MergeTuples();
 * 
 * -- input: 
 * -- ({(1, a),(2, b),(3, c),(4, d)})
 * input = LOAD 'input' AS (B: bag {T: tuple(v1:INT, v2:CHARARRAY)});
 * 
 * -- output: 
 * -- ((1,a, 2,b, 3, c, 4, d))
 * output = FOREACH input GENERATE MergeTuples(B); 
 * } 
 * </pre>
 * </p>
 * @author chenxm
 *
 */
public class MergeTuples extends SimpleEvalFunc<Tuple>{
	public Tuple call(DataBag input){
		Tuple output = TupleFactory.getInstance().newTuple();
		for (Tuple t : input){
			for(Object o : t)
				output.append(o);
		}
		return output;
	}
}
