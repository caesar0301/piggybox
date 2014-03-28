package com.piggybox.bags;


/**
 * Generate the number of occurrence times of each tuple in a bag regarding to specific field.
 * A bag of flattened tuples with field value and count number is returned.
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
 * output = FOREACH input GENERATE CountEachBy(OneBag, '0'); 
 * 
 * -- output: 
 * -- ({(a,1),(b,2),(c,1)})
 * output = FOREACH input GENERATE CountEachBy(OneBag, '1'); 
 * } 
 * </pre>
 * </p>
 * 
 * @author chenxm
 *
 */
public class CountEachBy {

}
