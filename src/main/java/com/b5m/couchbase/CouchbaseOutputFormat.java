package com.b5m.couchbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * An OutputFormat that sends the reduce output to a Couchbase server.
 *
 * @author Paolo D'Apice
 */
public final class CouchbaseOutputFormat<K extends Text, V> extends OutputFormat<K, V> {

    private CouchbaseConfiguration conf;

    public CouchbaseOutputFormat(CouchbaseConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
        // ignored
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
        return new CouchbaseRecordWriter<K, V>(conf);
    }

}
