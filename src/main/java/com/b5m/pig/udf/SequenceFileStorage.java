package com.b5m.pig.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

/**
 * hadoop's SequenceFileOutputFormat
 * 
 * @author Kevin Lin
 */
public class SequenceFileStorage extends StoreFunc {
	private static final Log log = LogFactory.getLog(SequenceFileStorage.class);
	private RecordWriter<Text, Text> writer;

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new SequenceFileOutputFormat<Text, Text>();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple record) throws IOException {
		String uuid = (String) record.get(0);
		String doc = (String) record.get(1);
		try {
			writer.write(new Text(uuid), new Text(doc));
		} catch (InterruptedException e) {
			log.error("Interrupted", e);
			throw new IOException(e);
		}
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		SequenceFileOutputFormat.setOutputPath(job, new Path(location));
	}

}
