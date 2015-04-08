package com.piggybox.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * A Simple Text Loader to accept regular expression as delimiter, as of an extension to PigStorage.
 * @author chenxm
 */
public class STLRegex extends LoadFunc{
	protected RecordReader in = null;	
    private Pattern fieldDel = Pattern.compile("\t");
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    /**
     * Constructs a Pig loader that uses regular expressions.
     *
     * @param delimiter the regex string to separate fields.
     */
    public STLRegex() {
    	this("\t");
    }

    public STLRegex(String delimiter) {
        if (delimiter.length() == 0){
        	return;
        }
        try{
        	this.fieldDel = Pattern.compile(delimiter);
        } catch (PatternSyntaxException e) {
        	throw new RuntimeException(
        			"STLRegex delimeter must be a string as regex");
        }
    }

    @Override
    public Tuple getNext() throws IOException {
    	if (mProtoTuple == null )
    		mProtoTuple = new ArrayList<Object>();
    	
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone)
                return null;
            Text value = (Text) in.getCurrentValue();
            String buf = new String(value.getBytes(), "UTF-8");
            int len = buf.length();
            int start = 0;
            // Perform matching
            Matcher matcher = this.fieldDel.matcher(buf);
            for (int i = 0; i < len; i = start) {
                if (matcher.find(i)){
                	readField(buf, start, matcher.start());
                	start = matcher.end();
                } else {
                	break;
                }
            }
            // pick up the last field
            if (start <= len) {
                readField(buf, start, len);
            }
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            mProtoTuple = null;
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
            		PigException.REMOTE_ENVIRONMENT, e);
        }
    }
    
    private void readField(String buf, int start, int end) {
        if (start == end) {
            mProtoTuple.add(null);
        } else {
            mProtoTuple.add(buf.substring(start, end));
        }
    }

	@Override
    public InputFormat getInputFormat() {
        return new PigTextInputFormat();
    }

	@Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }
}
