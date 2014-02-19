package com.b5m.topic.model.UNspLSA;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.topic.Driver;
import com.b5m.topic.util.MatrixHelper;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class ExternalProcessor {
	private static final Logger log = LoggerFactory
			.getLogger(ExternalProcessor.class);
	private static final String DOC_PAIR = "DOC_PAIR";
	private static final String DOC_VECTOR = "DOC_VECTOR";
	private static final String UNPRE_DIR = "UNPRE_DIR";
	private static final String JOB_NAME = "POST_PROCESS";
	private static final String MAX_WORD_PER_TOPIC = "MAX_WORD_PER_TOPIC";
	private static final String DOC_TOPIC_NAME = "DOC_TOPIC";
	private static final String TOPIC_MAP_PATH = "TOPIC_MAP_PATH";

	public static void setTopicMapPath(Configuration conf, Path path) {
		conf.set(TOPIC_MAP_PATH, path.toString());
	}

	public static Path getTopicMapPath(Configuration conf) {
		return new Path(conf.get(TOPIC_MAP_PATH));
	}

	public static void setTopicTermPath(Configuration conf, Path path) {
		conf.set(DOC_TOPIC_NAME, path.toString());
	}

	public static Path getTopicTermPath(Configuration conf) {
		String sp = conf.get(DOC_TOPIC_NAME);
		if (null == sp) {
			return null;
		}
		return new Path(sp);
	}

	public static void setPreprocessorPath(Configuration conf, Path dir) {
		conf.set(UNPRE_DIR, dir.toString());
	}

	public static Path getPreprocessorPath(Configuration conf) {
		return new Path(conf.get(UNPRE_DIR));
	}

	public static void setDocVectorPath(Configuration conf, Path doc) {
		conf.set(DOC_VECTOR, doc.toString());
	}

	public static Path getDocVectorPath(Configuration conf) {
		return new Path(conf.get(DOC_VECTOR));
	}

	public static void setMaxWordsPerTopic(Configuration conf, int num) {
		conf.setInt(MAX_WORD_PER_TOPIC, num);
	}

	public static int getMaxWordsPerTopic(Configuration conf) {
		return conf.getInt(MAX_WORD_PER_TOPIC, 128);
	}

	public static void preprocess(Path input, Path output, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException,
			URISyntaxException {
		FileSystem fs = input.getFileSystem(conf);

		setPreprocessorPath(conf, output);
		Path indexPath = new Path(output, DOC_PAIR);
		Path matrixPath = new Path(output, DOC_VECTOR);
		setDocVectorPath(conf, matrixPath);
		SequenceFile.Writer indexWriter = SequenceFile.createWriter(fs, conf,
				indexPath, IntWritable.class, Text.class);
		SequenceFile.Writer matrixWriter = SequenceFile.createWriter(fs, conf,
				matrixPath, IntWritable.class, VectorWritable.class);
		int numDoc = 0;
		try {
			IntWritable docId = new IntWritable();
			for (Pair<Text, VectorWritable> record : new SequenceFileDirIterable<Text, VectorWritable>(
					input, PathType.LIST, PathFilters.logsCRCFilter(), null,
					true, conf)) {
				VectorWritable value = record.getSecond();
				docId.set(numDoc);
				indexWriter.append(docId, record.getFirst());
				matrixWriter.append(docId, value);
				numDoc++;
			}
			numDoc++;
			Driver.setNumDocs(conf, numDoc);
		} finally {
			Closeables.close(indexWriter, false);
			Closeables.close(matrixWriter, false);
		}
	}

	public static class PostProcessMapper extends
			Mapper<IntWritable, VectorWritable, Text, Text> {
		private List<String> docPair;
		private Map<Integer, Integer> topicMap;
		private int numLabelPerDoc;
		private int numLabel;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			Path preDir = getPreprocessorPath(conf);
			Path docPairPath = new Path(preDir, DOC_PAIR);
			List<Pair<Integer, String>> rows = Lists.newArrayList();
			int maxDoc = 0;
			for (Pair<IntWritable, Text> row : new SequenceFileIterable<IntWritable, Text>(
					docPairPath, false, conf)) {
				rows.add(Pair.of(row.getFirst().get(), row.getSecond()
						.toString()));
				if (maxDoc < row.getFirst().get()) {
					maxDoc = row.getFirst().get();
				}
			}
			maxDoc++;
			docPair = new Vector<String>(maxDoc);
			for (Pair<Integer, String> row : rows) {
				docPair.add(row.getFirst(), row.getSecond());
			}
			rows.clear();

			Path topicMapPath = getTopicMapPath(conf);
			topicMap = new LinkedHashMap<Integer, Integer>();
			if (topicMapPath.getFileSystem(conf).exists(topicMapPath)) {
				for (Pair<IntWritable, IntWritable> row : new SequenceFileIterable<IntWritable, IntWritable>(
						topicMapPath, conf)) {
					topicMap.put(row.getFirst().get(), row.getSecond().get());
				}
			}

			numLabelPerDoc = UNspLSADriver.getNumLabelPerDoc(conf);
			numLabel = UNspLSADriver.getNumTopics(conf) + topicMap.size();
		}

		@Override
		public void map(IntWritable key, VectorWritable value, Context context)
				throws IOException, InterruptedException {
			String doc = docPair.get(key.get());
			org.apache.mahout.math.Vector docTopicVector = value.get();
			org.apache.mahout.math.Vector labelVector = new RandomAccessSparseVector(
					numLabel);
			for (int i = 0; i < numLabelPerDoc; i++) {
				int maxIndex = docTopicVector.maxValueIndex();
				double maxValue = docTopicVector.getQuick(maxIndex);
				docTopicVector.setQuick(maxIndex, -0.1);
				int topicId = maxIndex;
				if (topicMap.containsKey(maxIndex)) {
					topicId = topicMap.get(maxIndex);
				}
				labelVector.set(topicId, labelVector.get(topicId) + maxValue);
			}

			labelVector = labelVector.divide(labelVector.zSum());
			String label = new String();
			for (Element e : labelVector.nonZeroes()) {
				label += Integer.toString(e.index()) + ","
						+ Double.toString(e.get()) + "\t";
			}
			context.write(new Text(doc), new Text(label));
		}
	}

	public static void postProcess(Path input, Path output, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		HadoopUtil.delete(conf, output);
		Path topicTermPath = new Path(input, TopicModel.TOPIC_TERM + "-r-*");
		Path topicTermDictPath = Driver.getDicPath(conf);
		Path topicMapPath = new Path(output, TOPIC_MAP_PATH);
		setTopicMapPath(conf, topicMapPath);

		mergeTopics(UNspLSADriver.getTopTermPath(conf),
				UNspLSADriver.getTopTermDict(conf), topicTermPath,
				topicTermDictPath, topicMapPath, conf);

		Path preDir = getPreprocessorPath(conf);
		Path docPair = new Path(preDir, DOC_PAIR);
		DistributedCache.addCacheFile(docPair.toUri(), conf);
		if (topicMapPath.getFileSystem(conf).exists(topicMapPath)) {
			DistributedCache.addCacheFile(topicMapPath.toUri(), conf);
		}

		Job job = new Job(conf);
		job.setJarByClass(ExternalProcessor.class);
		job.setJobName(JOB_NAME);

		Path modelPath = new Path(input, TopicModel.DOC_TOPIC + "-r-*");
		Path labelPath = new Path(output, "temp");
		FileInputFormat.setInputPaths(job, modelPath);
		FileOutputFormat.setOutputPath(job, labelPath);

		job.setMapperClass(PostProcessMapper.class);
		job.setReducerClass(Reducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		HadoopUtil.delete(conf, labelPath);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

		output.getFileSystem(conf).rename(new Path(labelPath, "part-r-00000"),
				UNspLSADriver.getTopDocLabel(conf));

	}

	public static void mergeTopics(Path topTermPath, Path topTermDictPath,
			Path topicTermPath, Path topicTermDictPath, Path topicMapPath,
			Configuration conf) throws IOException {
		FileSystem fs = topTermPath.getFileSystem(conf);
		Map<String, Integer> topTermDict = new LinkedHashMap<String, Integer>();
		Integer numTerm = 0;
		if (fs.exists(topTermDictPath)) {
			for (Pair<Text, IntWritable> row : new SequenceFileIterable<Text, IntWritable>(
					topTermDictPath, conf)) {
				topTermDict.put(row.getFirst().toString(), row.getSecond()
						.get());
				if (numTerm < row.getSecond().get()) {
					numTerm = row.getSecond().get();
				}
			}
			numTerm++;
		}

		List<String> topicTermDict = Lists.newArrayList();
		for (Pair<Text, IntWritable> row : new SequenceFileDirIterable<Text, IntWritable>(
				topicTermDictPath, PathType.GLOB, null, null, true, conf)) {
			topicTermDict.add(row.getFirst().toString());
		}

		Matrix topicTerm = MatrixHelper.matrixFromSequenceFile(conf,
				topicTermPath);
		int maxWordsPerTopic = getMaxWordsPerTopic(conf);
		List<List<Pair<Integer, Double>>> topics = Lists.newArrayList();

		for (int z = 0; z < topicTerm.numRows(); z++) {
			org.apache.mahout.math.Vector pGivenTopic = topicTerm.viewRow(z);

			List<Pair<Integer, Double>> topic = Lists.newArrayList();
			for (int w = 0; w < maxWordsPerTopic; w++) {
				int maxIndex = pGivenTopic.maxValueIndex();
				double maxValue = pGivenTopic.maxValue();
				pGivenTopic.setQuick(maxIndex, -0.1);

				String term = topicTermDict.get(maxIndex);
				if (topTermDict.containsKey(term)) {
					topic.add(Pair.of(topTermDict.get(term), maxValue));
				} else {
					topTermDict.put(term, numTerm);
					topic.add(Pair.of(numTerm, maxValue));
					numTerm++;
				}
			}
			topics.add(topic);
		}

		SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
				topTermDictPath, Text.class, IntWritable.class);
		Iterator<Map.Entry<String, Integer>> it = topTermDict.entrySet()
				.iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> v = it.next();
			out.append(new Text(v.getKey()), new IntWritable(v.getValue()));
		}
		Closeables.close(out, false);

		Matrix topTerm = MatrixHelper.matrixFromSequenceFile(conf, topTermPath,
				numTerm);

		int numTopic = topicTerm.numRows();
		double threshold = 1e-6; // to pick the most similar topic.

		List<org.apache.mahout.math.Vector> newTopTerm = Lists.newArrayList();
		List<Pair<Integer, Integer>> topMap = Lists.newArrayList();
		int existNumTopic = null == topTerm ? 0 : topTerm.numRows();
		for (int i = 0; i < existNumTopic; i++) {
			newTopTerm.add(topTerm.viewRow(i));
		}

		for (int z = 0; z < topics.size(); z++) {
			org.apache.mahout.math.Vector topic = new RandomAccessSparseVector(
					numTerm);
			List<Pair<Integer, Double>> termList = topics.get(z);
			for (Pair<Integer, Double> term : termList) {
				topic.set(term.getFirst(), term.getSecond());
			}
			if (null == topTerm) {
				newTopTerm.add(topic);
				numTopic++;
			} else {
				int existTopicId = MatrixHelper.fuzzyIndex(topTerm, topic,
						threshold);
				if (-1 == existTopicId) {
					newTopTerm.add(topic);
					existTopicId = numTopic;
					numTopic++;
				} else {
					newTopTerm.get(existTopicId).assign(topic, Functions.PLUS);
				}
				topMap.add(Pair.of(z, existTopicId));
			}
		}
		if (!topMap.isEmpty()) {
			SequenceFile.Writer topicMapOut = SequenceFile.createWriter(fs,
					conf, topicMapPath, IntWritable.class, IntWritable.class);
			for (int i = 0; i < topMap.size(); i++) {
				topicMapOut.append(new IntWritable(topMap.get(i).getFirst()),
						new IntWritable(topMap.get(i).getSecond()));
			}
			Closeables.close(topicMapOut, false);
		}

		SequenceFile.Writer topicTermOut = SequenceFile.createWriter(fs, conf,
				topTermPath, IntWritable.class, VectorWritable.class);
		for (int i = 0; i < newTopTerm.size(); i++) {
			topicTermOut.append(new IntWritable(i),
					new VectorWritable(newTopTerm.get(i)));
		}
		Closeables.close(topicTermOut, false);
	}
}
