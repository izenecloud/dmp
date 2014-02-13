package com.b5m.topic;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.topic.model.UNspLSA.UNspLSADriver;
import com.b5m.topic.model.UNspLSA.ExternalProcessor;
import com.b5m.topic.model.Util.TopicDriver;
import com.b5m.topic.util.DocumentProcessor;

public class TopicExtractor {
	private static final Logger log = LoggerFactory.getLogger(TopicExtractor.class);

	private static final int DEFAULT_NUM_TOPICS = 20;
	private static final int DEFAULT_MAX_ITERATION = 20;
	private static final int DEFAULT_NUM_LABEL_PER_DOC = 2;
	private static final float DEFAULT_CONVERAGE_THRESHOLD = (float) 0.991;

	private static final String uspLSA_MODEL = "uspLSA_model";
	private static final String uspLSA_DOC_TOPIC = "uspLSA_doc_topic";
	private static final String uspLSA_TOPICS = "uspLSA_topics";
	private static final String US_VECTOR = "unsupervised_vector";

	private static final String DOC_PATH = "document";

	public static void run(Path input, Path output, Configuration conf,
			Integer numTopics, Integer maxIteration, Integer numLabelPerDoc,
			Double converage, Path topicTermPath,
			Path topWeightTopicTermDictPath) throws Exception {
		log.info(
				"Begin to extact topic, numTopics:{}, maxIteraton:{}, number of labels per document:{}, converage:{}, top weight topic term:{}, top weight topic term dict:{}",
				numTopics, maxIteration, numLabelPerDoc, converage,
				topicTermPath, topWeightTopicTermDictPath);

		Path tempOutput = new Path(output.getParent(), "TMEP");

		Path docPath = new Path(tempOutput, DOC_PATH);
		DocumentProcessor.preprocess(input, docPath, conf);

		TopicDriver.setMaxIterations(conf,
				null == maxIteration ? DEFAULT_MAX_ITERATION : maxIteration);
		TopicDriver.setNumTopics(conf, null == numTopics ? DEFAULT_NUM_TOPICS
				: numTopics);
		TopicDriver.setConvergence(
				conf,
				null == converage ? DEFAULT_CONVERAGE_THRESHOLD : converage
						.floatValue());
		TopicDriver.setTopTermPath(conf, topicTermPath);
		TopicDriver.setTopTermDic(conf, topWeightTopicTermDictPath);
		TopicDriver.setNumLabelPerDoc(conf,
				null == numLabelPerDoc ? DEFAULT_NUM_LABEL_PER_DOC
						: numLabelPerDoc);
		TopicDriver.setTopDocLabel(conf, output);

		runUNspLSA(DocumentProcessor.getDocWordVectorPath(conf),
				DocumentProcessor.getDictionaryPath(conf), tempOutput, conf);
		HadoopUtil.delete(conf, tempOutput);

		log.info(
				"Finished topic extract procedure, each document's label is saved in:{}, top weight topic term:{} is updated, top weight topic term dict:{} is updated",
				output, topicTermPath, topWeightTopicTermDictPath);
	}

	public static void runUNspLSA(Path input, Path dictionary, Path dateOutput,
			Configuration conf) throws ClassNotFoundException, IOException,
			InterruptedException, URISyntaxException {
		Path usVector = new Path(dateOutput, US_VECTOR);
		ExternalProcessor.preprocess(input, usVector, conf);
		Path pLSAModel = new Path(dateOutput, uspLSA_MODEL);
		Path pLSATempModel = new Path(dateOutput, uspLSA_TOPICS);
		Path docTopic = new Path(dateOutput, uspLSA_DOC_TOPIC);
		UNspLSADriver.setDictPath(conf, dictionary);
		UNspLSADriver.setTempModelPath(conf, pLSATempModel);
		UNspLSADriver.setFinalModelPath(conf, pLSAModel);
		UNspLSADriver.run(conf, ExternalProcessor.getDocVectorPath(conf),
				pLSAModel);
		ExternalProcessor.postProcess(pLSAModel, docTopic, conf);
	}

	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path topicTermPath = new Path(args[2]);
		Path topWeightTopicTermDictPath = new Path(args[3]);
		run(input, output, new Configuration(), null, null, null, null, topicTermPath, topWeightTopicTermDictPath);
	}
}
