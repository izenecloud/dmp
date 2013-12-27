package com.b5m.pig.udf;

import com.b5m.couchbase.CouchbaseConfiguration;
import com.b5m.couchbase.CouchbaseOutputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Pig output function to Couchbase.
 *
 * @author Paolo D'Apice
 */
public final class CouchbaseStorage extends StoreFunc {

    private final static Log log = LogFactory.getLog(CouchbaseStorage.class);

    private final CouchbaseConfiguration conf;

    private RecordWriter<Text, Object> writer = null;

    public CouchbaseStorage(String uris, String bucket) {
        this(uris, bucket, "");
    }

    public CouchbaseStorage(String uris, String bucket, String password) {
        conf = new CouchbaseConfiguration(uris, bucket, password);
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new CouchbaseOutputFormat<Text, Object>(conf);
    }

    @Override
    public void setStoreLocation(String location,Job job) throws IOException {
        // nothing to do because we are storing into Couchbase
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        // TODO check that the tuple has the correct schema
    }

    /*
     * Here the RecordWriter will actually be an instance of
     * CouchbaseRecordWriter<K,V> as returned by
     * CouchbaseOutputFormat#getRecordWriter().
     * Hence we can safely suppress this warning.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        /*
         * TODO rewrite this after checking the schema:
         * should have a textual key (likely first value of the tuple
         * and all other records in tuple should be packed and serialized
         * to Json.
         */

        String key = (String) tuple.get(0);
        Object value = tuple.get(1);

        if (log.isDebugEnabled())
            log.debug("writing K=" + key + ",V=" + value);

        try {
            writer.write(new Text(key), value);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
            throw new IOException(e);
        }
        System.out.println(tuple.toString());
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        // TODO
    }
}


