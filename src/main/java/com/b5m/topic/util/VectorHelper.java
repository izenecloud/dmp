package com.b5m.topic.util;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

public class VectorHelper {
	public static Vector vectorFromSequenceFile(Configuration conf, Path file) {
		int numTopics = -1;
		List<Pair<Integer, Double>> rows = Lists.newArrayList();
		for (Pair<IntWritable, DoubleWritable> row : new SequenceFileDirIterable<IntWritable, DoubleWritable>(
				file, PathType.GLOB, null, null, true, conf)) {
			rows.add(Pair.of(row.getFirst().get(), row.getSecond().get()));
			numTopics = Math.max(numTopics, row.getFirst().get());
		}
		numTopics++;
		Vector pz = new DenseVector(numTopics);
		for (Pair<Integer, Double> pair : rows) {
			pz.set(pair.getFirst(), pair.getSecond());
		}
		return pz;
	}
}
