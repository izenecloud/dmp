package com.b5m.maxent;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * An OutputFormat for training a Maximum Entropy classifier.
 * 
 * @author Paolo D'Apice
 */
public final class MaxEntOutputFormat extends OutputFormat<Text, Text> {

    private final static Log log = LogFactory.getLog(MaxEntOutputFormat.class);

    private final int iterations;
    private final boolean smoothing;

    public MaxEntOutputFormat(int iterations, boolean smoothing) {
        this.iterations = iterations;
        this.smoothing = smoothing;
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
        return new MaxEntRecordWriter(iterations, smoothing);
    }

    @Override
    public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
        Path path = FileOutputFormat.getOutputPath(context);
        if (log.isDebugEnabled()) log.debug("path: " + path);

        if (path == null) {
            String message = "Output path is not set";
            log.error(message);
            throw new InvalidJobConfException(message);
        }

        if (path.getFileSystem(context.getConfiguration()).exists(path)) {
            String message = String.format("Output path %s already exists", path);
            log.error(message);
            throw new FileAlreadyExistsException(message);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
        final Path path = FileOutputFormat.getOutputPath(context);
        return new OutputCommitter() {

            @Override
            public void setupJob(JobContext context)
            throws IOException {
                // nothing to do
            }

            @Override
            public void setupTask(TaskAttemptContext context) 
            throws IOException {
                // nothing to do
            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext context)
            throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext context)
            throws IOException {
                // nothing to do
            }

            @Override
            public void abortTask(TaskAttemptContext context)
            throws IOException {
                path.getFileSystem(context.getConfiguration()).delete(path, false);
            }
        };
    }
    
}
