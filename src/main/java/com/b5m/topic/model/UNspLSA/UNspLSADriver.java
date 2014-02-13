package com.b5m.topic.model.UNspLSA;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.spectral.common.IntDoublePairWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.topic.model.Util.TopicDriver;
import com.b5m.topic.model.Util.VectorSum;

public class UNspLSADriver extends TopicDriver {

	private static final Logger log = LoggerFactory.getLogger(UNspLSADriver.class);

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}

	public static int run(Configuration baseConf, Path input, Path output)
			throws ClassNotFoundException, IOException, InterruptedException {
		Path tempModelPath = getTempModelPath(baseConf);

		int maxIterations = getMaxIterations(baseConf);
		Path currPath = null;

		for (int i = 0; i < maxIterations; i++) {
			log.info("About to run iteration {} of {}", i, maxIterations);

			Configuration conf = new Configuration(baseConf);

			currPath = new Path(tempModelPath, Integer.toString(i));
			Path modelPath;
			if (i == 0) {
				modelPath = getInitModelPath(conf);
				if (null != modelPath) {
					DistributedCache.addCacheFile(modelPath.toUri(), conf);
				}

			} else {
				modelPath = new Path(tempModelPath, Integer.toString(i - 1));
				DistributedCache.addCacheFile(modelPath.toUri(), conf);
				setInitModelPath(conf, modelPath);
			}

			Job job = new Job(conf);
			job.setJarByClass(UNspLSADriver.class);
			FileInputFormat.setInputPaths(job, input);
			FileOutputFormat.setOutputPath(job, currPath);

			job.setMapperClass(EMapper.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(IntPairWritable.class);
			job.setMapOutputValueClass(VectorWritable.class);
			job.setCombinerClass(VectorSum.class);
			job.setReducerClass(MReducer.class);

			MultipleOutputs.addNamedOutput(job, TopicModel.DOC_TOPIC, SequenceFileOutputFormat.class,
					IntWritable.class, VectorWritable.class);
			MultipleOutputs.addNamedOutput(job, TopicModel.TOPIC_TERM, SequenceFileOutputFormat.class,
					IntWritable.class, VectorWritable.class);

			HadoopUtil.delete(conf, currPath);
			boolean succeeded = job.waitForCompletion(true);
			if (!succeeded) {
				throw new IllegalStateException("Job failed!");
			}
			if (null ==	modelPath ) {
				continue;
			}

			if (TopicModel.convergence(modelPath, currPath, conf)) {
				log.info("The TopicModel is converagence!!");
				break;
			}
		}
		
		{
			Path finalModelPath = getFinalModelPath(baseConf);
			FileSystem fs = finalModelPath.getFileSystem(baseConf);
			fs.rename(currPath, finalModelPath);
		}
		return 0;
	}
}
