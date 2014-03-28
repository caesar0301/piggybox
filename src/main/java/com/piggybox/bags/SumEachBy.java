package com.piggybox.bags;


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
public class SumEachBy {

}
