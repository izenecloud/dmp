package com.b5m.topic.model.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;

public abstract class TopicDriver extends AbstractJob {
	private static final String NUM_TOPICS = "numTopics";
	private static final String NUM_TERMS = "numTerms";
	private static final String NUM_DOCS = "numDocs";
	protected static final String NUM_CATE = "numCategory";

	private static final String INIT_MODEL_PATH = "init-model-path";
	private static final String FINAL_MODEL_PATH = "final-model-path";
	private static final String TMEP_MODEL_PATH = "temp-model-path";
	private static final String MAX_ITERATIONS = "maxIterations";
	private static final String CONVERGENCE = "convergence";
	private static final String DICT_PATH = "dict_path";
	private static final String TOP_TERM_PATH = "TOP_TERM_PATH";
	private static final String TOP_TERM_DICT = "TOP_TERM_DICT";
	private static final String TOP_DOC_LABEL = "TOP_DOC_LABEL";
	private static final String NUM_LABEL_PER_DOC = "NUM_LABEL_PER_DOC";

	public static void setNumLabelPerDoc(Configuration conf, int num) {
		conf.setInt(NUM_LABEL_PER_DOC, num);
	}
	
	public static int getNumLabelPerDoc(Configuration conf) {
		return conf.getInt(NUM_LABEL_PER_DOC, 1);
	}
	
	public static void setTopDocLabel(Configuration conf, Path path) {
		conf.set(TOP_DOC_LABEL, path.toString());
	}
	
	public static Path getTopDocLabel(Configuration conf) {
		return new Path(conf.get(TOP_DOC_LABEL));
	}
	
	
	public static void setTopTermPath(Configuration conf, Path path) {
		conf.set(TOP_TERM_PATH, path.toString());
	}
	
	public static Path getTopTermPath(Configuration conf) {
		return new Path(conf.get(TOP_TERM_PATH));
	}
	
	public static void setTopTermDic(Configuration conf, Path path) {
		conf.set(TOP_TERM_DICT, path.toString());
	}
	
	public static Path getTopTermDict(Configuration conf) {
		return new Path(conf.get(TOP_TERM_DICT));
	}
	
	public static void setConvergence(Configuration conf, float con) {
		conf.setFloat(CONVERGENCE, con);
	}

	public static float getConvergence(Configuration conf) {
		return conf.getFloat(CONVERGENCE, 0);
	}

	public static void setTempModelPath(Configuration conf, Path path) {
		conf.set(TMEP_MODEL_PATH, path.toString());
	}

	public static Path getTempModelPath(Configuration conf) {
		return new Path(conf.get(TMEP_MODEL_PATH));
	}

	public static void setInitModelPath(Configuration conf, Path modelPath) {
		conf.set(INIT_MODEL_PATH, modelPath.toString());
	}

	public static Path getInitModelPath(Configuration conf) {
		String model = conf.get(INIT_MODEL_PATH);
		if (null == model) {
			return null;
		}
		return new Path(model);
	}

	public static void setFinalModelPath(Configuration conf, Path modelPath) {
		conf.set(FINAL_MODEL_PATH, modelPath.toString());
	}

	public static Path getFinalModelPath(Configuration conf) {
		String model = conf.get(FINAL_MODEL_PATH);
		if (null == model) {
			return null;
		}
		return new Path(model);
	}

	public static void setDictPath(Configuration conf, Path dict) {
		conf.set(DICT_PATH, dict.toString());
		int numTerms = -1;
		for (Pair<Text, IntWritable> row : new SequenceFileDirIterable<Text, IntWritable>(
				dict, PathType.GLOB, null, null, true, conf)) {
			numTerms = row.getSecond().get();
		}
		numTerms++;
		setNumTerms(conf, numTerms);
	}

	public static Path getDicPath(Configuration conf) {
		String s = conf.get(DICT_PATH);
		if (null == s) {
			return null;
		}
		return new Path(s);
	}

	public static void setMaxIterations(Configuration conf, int maxIterations) {
		conf.setInt(MAX_ITERATIONS, maxIterations);
	}

	public static int getMaxIterations(Configuration conf) {
		return conf.getInt(MAX_ITERATIONS, 0);
	}

	public static void setNumTerms(Configuration conf, int numTerms) {
		conf.setInt(NUM_TERMS, numTerms);
	}

	public static int getNumTerms(Configuration conf) {
		return conf.getInt(NUM_TERMS, 10);
	}

	public static void setNumTopics(Configuration conf, int numTopics) {
		conf.setInt(NUM_TOPICS, numTopics);
	}

	public static int getNumTopics(Configuration conf) {
		return conf.getInt(NUM_TOPICS, 0);
	}

	public static void setNumDocs(Configuration conf, int numDocs) {
		conf.setInt(NUM_DOCS, numDocs);
	}

	public static int getNumDocs(Configuration conf) {
		return conf.getInt(NUM_DOCS, 0);
	}

	public static void setNumClasses(Configuration conf, int numCate) {
		conf.setInt(NUM_CATE, numCate);
	}

	public static int getNumClasses(Configuration conf) {
		return conf.getInt(NUM_CATE, 0);
	}
}
