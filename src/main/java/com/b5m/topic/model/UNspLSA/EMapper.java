package com.b5m.topic.model.UNspLSA;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.spectral.common.IntDoublePairWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class EMapper extends Mapper<IntWritable, VectorWritable, IntPairWritable, VectorWritable>{
	  private static final Logger log = LoggerFactory.getLogger(EMapper.class);
	  private TopicModel model;
	  private int numTopics;
	  private int numTerms;
	  public static final int ZW_KEY = 0;
	  public static final int DZ_KEY = 1;
	@Override
	  protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	  
	    log.info("Initializing topic model...");
	    Path modelPath = UNspLSADriver.getInitModelPath(conf);
	    if (modelPath != null) {
	    	model = new TopicModel(conf, modelPath);
	    }
	    else {
	    	model = new TopicModel(conf);
	    }
	    numTopics = UNspLSADriver.getNumTopics(conf);
		numTerms = UNspLSADriver.getNumTerms(conf);
	  }

	  @Override
	  public void map(IntWritable key, VectorWritable document, Context context)
	      throws IOException, InterruptedException {
		  	int docId = key.get();
			Matrix posterior = new SparseRowMatrix(numTopics, numTerms);
			model.trainDocTopicModel(document.get(), docId, posterior);

			// to estimate p(z|d)
			// key: docid
			// value: <topic, weight>,....
			Vector topicPairs = new DenseVector(numTopics);
			for (Element e : posterior.viewRow(0).nonZeroes()) {
				Vector c = posterior.viewColumn(e.index());
				for (int z = 0; z < numTopics; z++) {
					topicPairs.setQuick(z, topicPairs.getQuick(z) + c.getQuick(z));
				}
			}

			context.write(new IntPairWritable(DZ_KEY, docId), new VectorWritable(
					topicPairs));

			// to estimate p(w|z)
			// key: topicId
			// value: <wordId, weight>
			for (int z = 0; z < numTopics; z++) {
				context.write(new IntPairWritable(ZW_KEY, z), new VectorWritable(
						posterior.viewRow(z)));
			}
	  }

	  @Override
	  protected void cleanup(Context context) throws IOException, InterruptedException {
	    log.info("Cleanup EMapper...");
	  }
}
