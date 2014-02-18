package com.b5m.pig.udf;

import com.b5m.maxent.MaxEntOutputFormat;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Train a Maximum Entropy classifier.
 * 
 * @author Paolo D'Apice
 */
public final class MaxEntClassifierBuilder extends StoreFunc {

    private final static Log log = LogFactory.getLog(MaxEntClassifierBuilder.class);

    private final int iterations;
    private final boolean smoothing;

    private RecordWriter<Text, Text> writer;

    public MaxEntClassifierBuilder() {
        this("100", "false");
    }

    public MaxEntClassifierBuilder(String iterations, String smoothing) {
        this.iterations = Integer.valueOf(iterations);
        this.smoothing = Boolean.valueOf(smoothing);
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new MaxEntOutputFormat(iterations, smoothing);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        if (log.isDebugEnabled()) log.debug("schema: " + schema);
        ResourceFieldSchema[] fields = schema.getFields();

        if (fields.length != 2) {
            String message = String.format("Expected input tuple of size 2, received %d",
                                           fields.length);
            throw new IOException(message);
        }

        byte field1 = fields[0].getType();
        byte field2 = fields[1].getType();
        if (field1 != DataType.CHARARRAY || field2 != DataType.CHARARRAY) {
            String message = String.format("Expected schema (chararray, chararray), "
                                          +"received (%s, %s)",
                                           DataType.findTypeName(field1),
                                           DataType.findTypeName(field2));
            throw new IOException(message);
        }
    }    

    /*
     * Here the RecordWriter will actually be an instance of
     * MaxEntRecordWriter as returned by
     * MaxEntOutputFormat#getRecordWriter().
     * Hence we can safely suppress this warning.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        String context = (String) tuple.get(0);
        String outcome = (String) tuple.get(1);

        try {
            if (log.isTraceEnabled()) {
                String msg = String.format("outcome: %s\tcontext: %s", outcome, context);
                log.trace(msg);
            }

            writer.write(new Text(outcome), new Text(context));
        } catch (InterruptedException ex) {
            log.error("Interrupted", ex);
            throw new IOException(ex);
        }
    }

}
