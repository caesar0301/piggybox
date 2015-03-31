package com.piggybox.test;

import com.piggybox.omnilab.wifi.RawLogCleanse;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.*;

public class TestRawLogCleanse {
    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testLogCleanse() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("wifi-syslog.txt").getFile());
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

        RawLogCleanse rlc  = new RawLogCleanse();

        String line;
        while ( (line = br.readLine()) != null) {
            rlc.call(line);
        }
    }
}
