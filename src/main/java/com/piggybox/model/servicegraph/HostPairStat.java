package com.piggybox.model.servicegraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math.stat.StatUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Get host pair statistics from a bag of host pair tuples (host1, host2, time_gap, support)
 * return a tuple of host pair stat (support, min, percentile14, percentile34, max, mean, variance)
 * @author chenxm
 *
 */
public class HostPairStat extends EvalFunc<Tuple> {
    // tuple maker
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if ( input == null || input.size() == 0 || input.get(0) == null )
            return null;
        Object o = input.get(0);
        if ( !(o instanceof DataBag) )
            throw new IOException("Expected input to be DataBag, but got " + o.getClass().getName());
        // do work
        List<Double> timeGaps = new LinkedList<Double>();
        Iterator<Tuple> it = ((DataBag) o).iterator(); 
        while ( it.hasNext() ){
            progress();     // heart-beat to task tracker
            Tuple tuple = it.next();
            // make sure that we get right types of tuple fields.
            long timeVal = (Long) tuple.get(2);
            long support = (Long) tuple.get(3);
            for ( int i = 0; i < support; i++ )
                timeGaps.add(new Double(timeVal));
        }
        // Convert double list to array
        double[] timeGapArray = listToArray(timeGaps);
        Tuple newTuple = mTupleFactory.newTuple();
        newTuple.append(timeGapArray.length);                       // support of host pair
        newTuple.append(StatUtils.min(timeGapArray));               // min of hp time gap
        newTuple.append(StatUtils.percentile(timeGapArray, 0.25));  // 0.25-p of hp time gap
        newTuple.append(StatUtils.percentile(timeGapArray, 0.75));  // 0.75-p of hp time gap
        newTuple.append(StatUtils.max(timeGapArray));               // max of hp time gap
        newTuple.append(StatUtils.mean(timeGapArray));              // mean of hp time gap
        newTuple.append(StatUtils.variance(timeGapArray));          // variance of hp time gap
        return newTuple;
    }
    
    private double[] listToArray(List<Double> vaList){
        double[] ret = new double[vaList.size()];
        for ( int i = 0; i < vaList.size(); i++ )
            ret[i] = vaList.get(i).doubleValue();
        return ret;
    }
    
    /**
     * Define output schema which can be understood by PIG and output human-readable 
     * text with DESCRIBE.
     */
    public Schema outputSchema(Schema input) {
        try{
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_support", input), DataType.LONG));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_min", input), DataType.DOUBLE));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_percentile14", input), DataType.DOUBLE));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_percentile34", input), DataType.DOUBLE));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_max", input), DataType.DOUBLE));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_mean", input), DataType.DOUBLE));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_var", input), DataType.DOUBLE));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_hostpairstat", input), tupleSchema, DataType.TUPLE));
        }catch (Exception e){
            return null;
        }
    }

}
