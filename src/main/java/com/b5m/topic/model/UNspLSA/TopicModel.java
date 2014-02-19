package com.b5m.topic.model.UNspLSA;

import com.b5m.topic.util.MatrixHelper;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class TopicModel implements Configurable, Iterable<MatrixSlice> {
	
	public static final String TOPIC_TERM = "TOPICTERM";
	public static final String DOC_TOPIC  = "DOCTOPIC";

	/*
	 * symbol convention: document- d topic - z word - w
	 */

	/*
	 * This implement is based on asymmetric model.
	 */
	
	private final Matrix topicTerm; // p(w | z)
	// to do, this should be initialized differently per mapper.
	private final Matrix documentTopic; // p(z | d)
	private final int numTopics;
	private final int numTerms;
	private final int numDocs;


	private Configuration conf;

	
	public TopicModel(Configuration conf, Path modelpath) throws IOException {
		Path topicTermPath = new Path(modelpath, TOPIC_TERM + "-r-*");
		topicTerm = MatrixHelper.matrixFromSequenceFile(conf, topicTermPath);
		Path docTopicPath = new Path(modelpath, DOC_TOPIC + "-r-*");
		documentTopic = MatrixHelper.matrixFromSequenceFile(conf, docTopicPath);
		
		numTopics = UNspLSADriver.getNumTopics(conf);
		numTerms = UNspLSADriver.getNumTerms(conf);
		numDocs = UNspLSADriver.getNumDocs(conf);
	}

	public TopicModel(Configuration conf) {
		numTopics = UNspLSADriver.getNumTopics(conf);
		numTerms = UNspLSADriver.getNumTerms(conf);
		numDocs = UNspLSADriver.getNumDocs(conf);
		Random rand = RandomUtils.getRandom(1234L);
		
		topicTerm = new DenseMatrix(numTopics, numTerms);
		for (int i = 0; i < numTopics; i++) {
			Vector row = topicTerm.viewRow(i);
			for (Element e : row.all()) {
				e.set(rand.nextDouble());
			}
		}
		documentTopic = new DenseMatrix(numDocs, numTopics);
		for (int i = 0; i < numDocs; i++) {
			Vector row = documentTopic.viewRow(i);
			for (Element e : row.all()) {
				e.set(rand.nextDouble());
			}
		}
	}

	public void trainDocTopicModel(Vector document, int docId,
			Matrix posterior) {
		topicPosterior(document, docId, posterior);
		normalizeByTopic(posterior);
		for (Element e : document.nonZeroes()) {
			int index = e.index();
			double w = e.get();
			for (int z = 0; z < numTopics; z++) {
				Vector pGivenTopic = posterior.viewRow(z);
				pGivenTopic.setQuick(index, pGivenTopic.getQuick(index) * w);
			}
		}
	}

	private void topicPosterior(Vector document, int docId,
			Matrix posteriors) {
		Vector dtopics = documentTopic.viewRow(docId);
		for (int z = 0; z < numTopics; z++) {
			double wdz = dtopics.get(z); // w(z|d)
			Vector posterior = posteriors.viewRow(z);
			Vector pzw = topicTerm.viewRow(z);
			for (Element e : document.nonZeroes()) {
				int index = e.index();
				double wzw = pzw.get(index);
				double w = wdz * wzw;
				posterior.set(index, w);
			}
		}
	}

	private void normalizeByTopic(Matrix posteriors) {
		for (Element e : posteriors.viewRow(0).nonZeroes()) {
			double denominator = 0;
			int index = e.index();
			for (int z = 0; z < numTopics; z++) {
				denominator += posteriors.viewRow(z).getQuick(index);
			}
			for (int z = 0; z < numTopics; z++) {
				double w = posteriors.viewRow(z).getQuick(index);
				posteriors.viewRow(z).set(index, w / denominator);
			}
		}
	}

	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public Iterator<MatrixSlice> iterator() {
		return null;
	}
	
	public static boolean convergence(Path prevModel, Path currModel, Configuration conf) {
		boolean ret = false;
		Matrix preTopicTerm = MatrixHelper.matrixFromSequenceFile(conf, new Path(prevModel, TOPIC_TERM + "-r-*"));
		Matrix curTopicTerm = MatrixHelper.matrixFromSequenceFile(conf, new Path(currModel, TOPIC_TERM + "-r-*"));
		
		float threshold = UNspLSADriver.getConvergence(conf);
		if (threshold < MatrixHelper.similarity(preTopicTerm, curTopicTerm)) {
			ret = true;
		}
		return ret;
	}
}
