package com.piggybox.bags;


import com.piggybox.utils.SimpleEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Generate the sum of a value field with respect to a key field in a bag of tuples.
 * A bag of flattened tuples of key field and summed value is returned.
 * <p>
 * Example:
 * <pre>
 * {@code
 * DEFINE SumEachBy com.piggybox.bags.SumEachBy();
 * 
 * -- input: 
 * -- ({(A, 5),(A, 6),(C, 7),(B, 8)})
 * input = LOAD 'input' AS (OneBag: bag {T: tuple(v1:CHARARRAY, v2:INT)});
 * 
 * -- output: 
 * -- ({(A,11),(C,7),(B,8)})
 * output = FOREACH input GENERATE SumEachBy(OneBag, '0', '1'); 
 * } 
 * </pre>
 * </p>
 * 
 * @author chenxm
 *
 */
public class SumEachBy extends SimpleEvalFunc<DataBag> {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    public DataBag call(DataBag input, Integer target, Integer value ) throws ExecException {

        // get sum stat
        Map<String, Double> sumAll = new HashMap<String, Double>();
        for ( Tuple t : input ) {
            String k = t.get(target).toString();
            Double v = new Double(t.get(value).toString());
            if ( ! sumAll.containsKey(k) )
                sumAll.put(k, 0.0);
            sumAll.put(k, sumAll.get(k) + v);
        }

        // return databag
        DataBag output = bagFactory.newDefaultBag();
        for ( String k : sumAll.keySet() ) {
            Tuple new_tuple = tupleFactory.newTuple(k);
            new_tuple.append(sumAll.get(k));
            output.add(new_tuple);
        }

        return output;
    }

}
