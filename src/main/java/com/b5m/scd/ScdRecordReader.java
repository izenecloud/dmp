package com.b5m.scd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * A RecordReader for SCD files.
 *
 * @author Paolo D'Apice
 */
final class ScdRecordReader extends RecordReader<Text, MapWritable> {

    private final static Log log = LogFactory.getLog(ScdRecordReader.class);

    private ScdFileReader reader;
    private Document document;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration job = context.getConfiguration();

        // open the file
        final Path file = fileSplit.getPath();
        final FileSystem fs = file.getFileSystem(job);
        FSDataInputStream inputStream = fs.open(file);
        reader = new ScdFileReader(inputStream);

        if (log.isDebugEnabled()) log.debug("opened reader to: " + file);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean val = reader.nextDocument();
        if (val) document = reader.getCurrentDocument();
            
        return val;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        String docid = document.entries.get(0).value;
        return new Text(docid);
    }

    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException {
        MapWritable fields = new MapWritable();
        for (Entry e : document.entries)
            fields.put(new Text(e.getTagName()), new Text(e.value));
        return fields;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0f; // cannot know how many documents
    }

    @Override
    public void close() throws IOException {
        if (reader != null) reader.close();
    }
    
}
