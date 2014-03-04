package com.piggybox.model.servicegraph;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * UDF to extract graph from a bag of tuples (umac, request_time, host, uri)
 * @author chenxm
 *
 */
public class GenerateGraphHosts extends EvalFunc<DataBag> {
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if ( input == null || input.size() == 0 || input.get(0) == null )
            return null;
        try{
            Object o = input.get(0);
            if ( !(o instanceof DataBag) )
                throw new IOException("Expected input to be DataBag, but  got " + o.getClass().getName());
            progress();
            return extractLinks((DataBag) o);
        } catch (Exception e){
            return null;
        }
    }
    
    private class HttpMessage{
        public double requestTime;
        public String host;
        public String uri;
        public HttpMessage(double t, String h, String u){
        	//System.out.println("HttpMessage constructor called.");
            requestTime = t;
            host = h;
            uri = u;
        }
    }
    
    /**
     * Extract graph links (two vertex and a time gap in seconds) from data bag.
     * @param input
     * @return A bag of links.
     * @throws IOException
     */
    private DataBag extractLinks( DataBag input) throws IOException{
    	//System.out.println("extractLinks called.");
        try {
            DataBag output = mBagFactory.newDefaultBag();
            List<HttpMessage> inputMessages = new LinkedList<HttpMessage>();
            
            for ( Iterator<Tuple> it = input.iterator(); it.hasNext();){
                Tuple tuple = it.next();
                HttpMessage newHttp = new HttpMessage((Double)tuple.get(1), (String)tuple.get(2), (String)tuple.get(3));
                inputMessages.add(newHttp);
                progress();
            }
            
            // Sort messages by request time
            Collections.sort(inputMessages, new Comparator<HttpMessage>() {
                @Override
                public int compare(HttpMessage o1, HttpMessage o2) {
                    if ( o1.requestTime == o2.requestTime )
                        return 0;
                    return o1.requestTime > o2.requestTime ? 1 : -1;
                }
            });
            
            // Generate graph link from consecutive two requests.
            // Vertex of the link are hosts/uris if their time gap is smaller than 1 second.
            for ( int i = 0; i < inputMessages.size(); i++ ){
                progress();
                HttpMessage refMessage = inputMessages.get(i);
                for ( int j = i+1; j < inputMessages.size(); j++ ){
                    Tuple newTuple = mTupleFactory.newTuple();
                    HttpMessage checkMessage = inputMessages.get(j);
                    newTuple.append(refMessage.host);
                    newTuple.append(checkMessage.host);
                    newTuple.append(checkMessage.requestTime - refMessage.requestTime);
                    output.add(newTuple);
                    if ( checkMessage.requestTime - refMessage.requestTime >= 1 ) {
                        break;
                    }
                }
            }
            return output;
        } catch (ExecException ee) {
            throw new ExecException("Unknown error to extract links - " + ee.getMessage());
        }  
    }

    /**
     * Define output schema which can be understood by PIG and output human-readable 
     * text with DESCRIBE.
     */
    public Schema outputSchema(Schema input) {
    	try{
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_host1", input), DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_host2", input), DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_time", input), DataType.DOUBLE));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase() + "_hostgraph", input), tupleSchema, DataType.TUPLE));
        }catch (Exception e){
            return null;
        }
    }
}
