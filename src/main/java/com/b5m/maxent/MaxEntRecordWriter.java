package com.b5m.maxent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import opennlp.maxent.GIS;
import opennlp.maxent.GISModel;
import opennlp.maxent.io.SuffixSensitiveGISModelWriter;
import opennlp.model.EventStream;
import opennlp.model.OnePassDataIndexer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
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
final class MaxEntRecordWriter extends RecordWriter<Text, Text> {

    private final static Log log = LogFactory.getLog(MaxEntRecordWriter.class);

    private final MaxEntEventGenerator generator = new MaxEntEventGenerator();

    private final int iterations;
    private final boolean smoothing;
    
    private final Path file;
    private final Writer writer;

    MaxEntRecordWriter(int iterations, boolean smoothing,
            TaskAttemptContext context) throws IOException {
        this.iterations = iterations;
        this.smoothing = smoothing;

        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        this.file = new Path(FileOutputFormat.getUniqueFile(context, "maxent-train", ".txt"));
        this.writer = new BufferedWriter(new OutputStreamWriter(fs.create(file)));

        if (log.isInfoEnabled())
            log.info("Using temporary file: " + file);
    }
    
    @Override
    public void write(Text key, Text value)
    throws IOException, InterruptedException {
        String outcome = key.toString();
        String context = value.toString();
        
        writer.write(generator.newEventString(outcome, context));
    }

    @Override
    public void close(TaskAttemptContext context)
    throws IOException, InterruptedException {
        if (log.isDebugEnabled())
            log.debug("finished writing");

        writer.close();

        if (log.isInfoEnabled())
            log.info("Training model ...");

        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        Reader reader = new InputStreamReader(fs.open(file));
        EventStream eventStream = new FileEventStream(reader);

        GISModel model = GIS.trainModel(
                iterations,
                new OnePassDataIndexer(eventStream),
                smoothing);

        if (log.isInfoEnabled())
            log.info("Model trained");
        
        reader.close();
        fs.delete(file, true);

        if (log.isInfoEnabled())
            log.info("Deleted temporary file: " + fs.exists(file));

        Path path = FileOutputFormat.getOutputPath(context);
        File modelFile = new File(path.toUri());
        new SuffixSensitiveGISModelWriter(model, modelFile).persist();

        if (log.isInfoEnabled())
            log.info("Model written to file: " + modelFile);
    }

}
