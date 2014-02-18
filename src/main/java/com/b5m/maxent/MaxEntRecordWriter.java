package com.b5m.maxent;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import opennlp.maxent.GIS;
import opennlp.maxent.GISModel;
import opennlp.maxent.io.GISModelWriter;
import opennlp.maxent.io.SuffixSensitiveGISModelWriter;
import opennlp.model.Event;
import opennlp.model.ListEventStream;
import opennlp.model.OnePassDataIndexer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * RecordWriter for training a Maximum Entropy classifier.
 * 
 * @author Paolo D'Apice
 */
public final class MaxEntRecordWriter extends RecordWriter<Text, Text> {

    private final static Log log = LogFactory.getLog(MaxEntRecordWriter.class);

    private final List<Event> events = Collections.synchronizedList(new LinkedList<Event>());
    private final MaxEntEventGenerator generator = new MaxEntEventGenerator();

    private final int iterations;
    private final boolean smoothing;

    public MaxEntRecordWriter(int iterations, boolean smoothing) {
        this.iterations = iterations;
        this.smoothing = smoothing;
    }
    
    @Override
    public void write(Text key, Text value)
    throws IOException, InterruptedException {
        String outcome = key.toString();
        String context = value.toString();
        
        events.add(generator.newEvent(outcome, context));
    }

    @Override
    public void close(TaskAttemptContext context)
    throws IOException, InterruptedException {
        if (log.isInfoEnabled())
            log.info("Training model ...");

        GISModel model = GIS.trainModel(
                iterations,
                new OnePassDataIndexer(new ListEventStream(events)),
                smoothing);

        if (log.isInfoEnabled())
            log.info("Model trained");
        
        Path path = FileOutputFormat.getOutputPath(context);
        File file = new File(path.toUri());
    
        GISModelWriter writer = new SuffixSensitiveGISModelWriter(model, file);
        writer.persist();

        if (log.isInfoEnabled())
            log.info("Model written to file: " + file);
    }
    
}
