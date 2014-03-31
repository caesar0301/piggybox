/*
 * Copyright 2012 LinkedIn Corp. and contributors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.piggybox.bags;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.piggybox.utils.SimpleEvalFunc;

/**
 * Returns the Nth (indexing from zero) tuple from a bag. Requires a second
 * parameter that will be returned if the bag is empty. This is a general
 * routine of datafu.pig.bags.FristTupleFromBag in datafu package.
 * 
 * Example:
 * 
 * <pre>
 *  {@code
 *  define NthTupleFromBag com.piggybox.bags.NthTupleFromBag();
 * 
 *  -- input:
 *  -- ({(a,1), (b,2)})
 *  input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 * 
 *  output = FOREACH input GENERATE NthTupleFromBag(1, B, null);
 * 
 *  -- output:
 *  -- (b,2)
 *  }
 *  
 *  @See  <a href="http://datafu.incubator.apache.org/docs/datafu/1.2.0/datafu/pig/bags/FirstTupleFromBag.html">
 * datafu.pig.bags.FristTupleFromBag</a>
 * 
 * </pre>
 */

public class NthTupleFromBag extends SimpleEvalFunc<Tuple> {
	private int index = 0;
	
	public Tuple call(int index, DataBag bag, Tuple defaultValue) throws IOException {
		int i = 0;
		for (Tuple t : bag) {
			if ( index == i)
				return t;
			i++;
		}
		return defaultValue;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(input.getField(index).schema);
		} catch (Exception e) {
			return null;
		}
	}
}
