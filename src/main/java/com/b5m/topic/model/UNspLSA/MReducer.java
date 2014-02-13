package com.b5m.topic.model.UNspLSA;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

class MReducer
		extends
		Reducer<IntPairWritable, VectorWritable, Writable, Writable> {
	private MultipleOutputs<Writable, Writable> mos;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		 mos = new MultipleOutputs<Writable, Writable>(context);
	}

	public void reduce(IntPairWritable key,
			Iterable<VectorWritable> values, Context context)
			throws IOException, InterruptedException {
		if (EMapper.DZ_KEY == key.getFirst()) {
			Vector pdz = null;
			for (VectorWritable value : values) {
				if (null == pdz) {
					pdz = value.get();
				} else {
					pdz.assign(value.get(), Functions.PLUS);
				}
			}
			double denominator = pdz.zSum();
			mos.write(TopicModel.DOC_TOPIC, new IntWritable(key.getSecond()),
					new VectorWritable(pdz.divide(denominator)));
		}
		if (EMapper.ZW_KEY == key.getFirst()) {
			Vector pzw = null;
			for (VectorWritable value : values) {
				if (null == pzw) {
					pzw = value.get();
				} else {
					pzw.assign(value.get(), Functions.PLUS);
				}
			}
			double denominator = pzw.zSum();
			mos.write(TopicModel.TOPIC_TERM, new IntWritable(key.getSecond()),
					new VectorWritable(pzw.divide(denominator)));
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
