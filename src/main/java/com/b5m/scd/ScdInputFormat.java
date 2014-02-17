package com.b5m.scd;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An InputFormat for SCD files.
 * 
 * @author Paolo D'Apice
 */
public final class ScdInputFormat extends FileInputFormat<Text, MapWritable> {

    @Override
    public RecordReader<Text, MapWritable>
    createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ScdRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false; // cannot arbitrarily split an SCD file
    }

}

