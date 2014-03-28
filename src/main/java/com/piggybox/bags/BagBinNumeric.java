package com.piggybox.bags;

/**
 * Given a bag of tuples whose first field is numeric (i.e. int, long, float, double),
 * this UDF generates a series of bins (inner bags) whose numeric ranges are not greater than a given range.
 * Note: this operation is order preserving.
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * DEFINE BagBinNumeric com.piggybox.bags.BagBinNumeric();
 * 
 * -- input: 
 * -- ({(1, a),(2, b),(3, c),(4, d)})
 * input = LOAD 'input' AS (B: bag {T: tuple(v1:INT, v2:CHARARRAY)});
 * 
 * -- output: 
 * -- ({{(1,a),(2,b)}, {(3, c),(4, d)}})
 * output = FOREACH input GENERATE BagBinNumeric(B, 2); 
 * } 
 * </pre>
 * </p>
 * 
 * @see <a href="http://datafu.incubator.apache.org/docs/datafu/1.2.0/datafu/pig/bags/BagSplit.html">The BagSplit UDF in datafu performs similar actions
 * on categorical values.</a>
 * 
 * @author chenxm
 *
 */
public class BagBinNumeric {

}
