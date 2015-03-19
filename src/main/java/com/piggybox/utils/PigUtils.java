package com.piggybox.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class PigUtils {
	public static List<Tuple> databagToList(DataBag bag) throws ExecException {
		List<Tuple> result = new ArrayList<Tuple>();
		for (Tuple t : bag) {
			result.add(t);
		}
		return result;
	}
}
