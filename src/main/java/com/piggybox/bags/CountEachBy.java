package com.piggybox.bags;


import com.piggybox.utils.SimpleEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Generate the number of occurrence times of each tuple in a bag regarding to specific field.
 * A bag of flattened tuples with field value and count number is returned.
 *
 * NB: In memory calculation.
 *
 * <p>
 * Example:
 * <pre>
 * {@code
 * DEFINE CountEachBy com.piggybox.bags.CountEachBy();
 * 
 * -- input: 
 * -- ({(A, a),(A, b),(C, b),(B, c)})
 * input = LOAD 'input' AS (OneBag: bag {T: tuple(v1:CHARARRAY, v2:CHARARRAY)});
 * 
 * -- output: 
 * -- ({(A,2),(C,1),(B,1)})
 * output = FOREACH input GENERATE CountEachBy(OneBag, 0);
 * 
 * -- output: 
 * -- ({(a,1),(b,2),(c,1)})
 * output = FOREACH input GENERATE CountEachBy(OneBag, 1);
 * } 
 * </pre>
 * </p>
 * 
 * @author chenxm
 *
 */
public class CountEachBy extends SimpleEvalFunc<DataBag> {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    public DataBag call(DataBag bag, Integer index) throws ExecException {

        // calculate frequency in memory
        Map<Object, Long> counts = new HashMap<Object, Long>();
        for (Tuple tuple : bag ) {
            String key = tuple.get(index).toString();
            if ( !counts.containsKey(key) ) {
                counts.put(key, new Long(0));
            }
            counts.put(key, counts.get(key) + 1);
        }

        // output to bag
        DataBag outBag = bagFactory.newDefaultBag();
        for ( Object key : counts.keySet() ) {
            Tuple new_tuple = tupleFactory.newTuple();
            new_tuple.append(key);
            new_tuple.append(counts.get(key));
            outBag.add(new_tuple);
        }

        return outBag;
    }
}
