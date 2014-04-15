package com.piggybox.bags;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Given a bag, return a new bag of one-field tuples which are joint by @sep.
 * If input bag is null, this UDF will return null; 
 *  
 * <p>
 * For example:
 * <pre>
 * {@code
 * DEFINE JoinEachBy com.piggybox.bags.JoinEachBy();
 * 
 * -- input: 
 * -- ({(hello, world), (How, are, you?)})
 * input = LOAD 'input' AS (OneBag: bag {T: tuple});
 * 
 * -- output: 
 * -- ({(hello-world), (How-are-you?)})
 * output = FOREACH input GENERATE JoinEachBy(OneBag, "-"); 
 * } 
 * </pre>
 * </p>
 * 
 * @author chenxm
 * @See  <a href="http://pig.apache.org/docs/r0.12.0/api/org/apache/pig/builtin/BagToString.html">
 * org.apache.pig.builtin.BagToString</a>: a recursive version which returns a unified string for given bag.
 */
public class JoinEachBy extends SimpleEvalFunc<DataBag> {
	
	public DataBag call(DataBag input, String sep){
		DataBag result = BagFactory.getInstance().newDefaultBag();
		if ( input == null )
			return null;
		for ( Tuple t : input ){
			String tupleStr = null;
			for ( Object item : t.getAll()){
				if ( tupleStr == null )
					tupleStr = item.toString();
				else
					tupleStr += sep + item.toString();
			}
			result.add(TupleFactory.getInstance().newTuple(tupleStr));
		}
		return result;
	}
}
