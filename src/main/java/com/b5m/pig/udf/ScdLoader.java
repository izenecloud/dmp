package com.b5m.pig.udf;

import com.b5m.scd.ScdInputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Load SCD files.
 * 
 * @author Paolo D'Apice
 */
public class ScdLoader extends LoadFunc implements LoadMetadata {

    private final static Log log = LogFactory.getLog(ScdLoader.class);

    private RecordReader reader = null;
    
    private final TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new ScdInputFormat();
    }

    @Override
    public void setUDFContextSignature(String signature) {
        // nothing to do
    }
    
    @Override
    public void prepareToRead(RecordReader reader, PigSplit ps) throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        Map<String, String> fields = new HashMap<String, String>();
        try {
            // no values
            if (!reader.nextKeyValue()) return null;
            
            // current value (we don't need the key)
            MapWritable value = (MapWritable) reader.getCurrentValue();
            for (Map.Entry<Writable, Writable> e : value.entrySet()) {
                fields.put(e.getKey().toString(), e.getValue().toString());
            }
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }

        Tuple tuple = tupleFactory.newTuple(fields);
        return tuple;
    }

    @Override
    public ResourceSchema getSchema(String string, Job job) throws IOException {
        Schema schema = Utils.getSchemaFromString("fields:map[chararray]");
        return new ResourceSchema(schema);
    }

    @Override
    public ResourceStatistics getStatistics(String string, Job job) throws IOException {
        // nothing to do
        return null;
    }

    @Override
    public String[] getPartitionKeys(String string, Job job) throws IOException {
        // nothing to do
        return null;
    }

    @Override
    public void setPartitionFilter(Expression exprsn) throws IOException {
        // nothing to do
    }
    
}
