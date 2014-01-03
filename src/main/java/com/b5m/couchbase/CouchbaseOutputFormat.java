package com.b5m.couchbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * An OutputFormat that sends the reduce output to a Couchbase server.
 *
 * @author Paolo D'Apice
 */
public final class CouchbaseOutputFormat<K extends Text, V> extends OutputFormat<K, V> {

    private final CouchbaseConfiguration conf;

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
        return new OutputCommitter() {
            @Override
            public void abortTask(TaskAttemptContext context) throws IOException {
                // nothing to do
            }
            @Override
            public void commitTask(TaskAttemptContext context) throws IOException {
                // nothing to do
            }
            @Override
            public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
                // nothing to do
                return false;
            }
            @Override
            public void setupJob(JobContext context) throws IOException {
                // nothing to do
            }
            @Override
            public void setupTask(TaskAttemptContext context) throws IOException {
                // nothing to do
            }
        };
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
        return new CouchbaseRecordWriter<K, V>(conf);
    }

}
