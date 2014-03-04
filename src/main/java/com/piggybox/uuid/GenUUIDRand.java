package com.piggybox.uuid;

import java.io.IOException;
import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * A wrapper to generate UUID randomly; the input can be empty.
 * @author chenxm
 */
public class GenUUIDRand extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		try {
			return UUID.randomUUID().toString();
		} catch (Exception e) {
			throw new IOException("Unknown exception ", e);
		}
	}
}
