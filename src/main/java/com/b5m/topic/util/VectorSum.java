package com.b5m.topic.util;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

public class VectorSum
		extends
		Reducer<IntPairWritable, VectorWritable, IntPairWritable, VectorWritable> {
	public void reduce(IntPairWritable key,
			Iterable<VectorWritable> values, Context context)
			throws IOException, InterruptedException {
		Vector sum = null;
		for (VectorWritable vector : values) {
			if (null == sum) {
				sum = vector.get();
			}else {
				sum.assign(vector.get(), Functions.PLUS);
			}
		}
		context.write(key, new VectorWritable(sum));
	}
}
