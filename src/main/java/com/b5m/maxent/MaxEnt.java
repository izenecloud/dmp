package com.b5m.maxent;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.maxent.ContextGenerator;
import opennlp.maxent.GIS;
import opennlp.maxent.PlainTextByLineDataStream;
import opennlp.maxent.io.GISModelWriter;
import opennlp.maxent.io.SuffixSensitiveGISModelWriter;
import opennlp.model.AbstractModel;
import opennlp.model.Event;
import opennlp.model.EventStream;
import opennlp.model.GenericModelReader;
import opennlp.model.MaxentModel;
import opennlp.model.OnePassDataIndexer;

public class MaxEnt {
	private static final Logger log = LoggerFactory.getLogger(MaxEnt.class);
	
	public static MaxEnt instance() {
		return new MaxEnt();
	}

	MaxentModel model;
	ContextGenerator cg = new MaxEntContextGenerator();

	static class MaxEnThread extends Thread {
		private MaxEnt model;
		private File file;
		long testCase = 0;
		long goodCase = 0;
		long badCase = 0;

		public MaxEnThread(MaxEnt maxent, File testFile) {
			model = maxent;
			file = testFile;
		}

		public void run() {
			try {
				FileReader datafr = new FileReader(file);
				EventStream es;
				es = new MaxEntEventStream(
						new PlainTextByLineDataStream(datafr));
				while (es.hasNext()) {
					Event event = es.next();
					String outcome = model.eval(event.getContext());
					if (event.getOutcome().equalsIgnoreCase(outcome)) {
						goodCase++;
					} else {
						badCase++;
					}
					testCase++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public long testCase() {
			return testCase;
		}

		public long goodCase() {
			return goodCase;
		}

		public long badCase() {
			return badCase;
		}
	}

	public MaxEnt() {
		model = null;
	}

	public MaxEnt(String modelFileName) {
		try {
			model = new GenericModelReader(new File(modelFileName)).getModel();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadModel(String modelFileName) {
		try {
			model = new GenericModelReader(new File(modelFileName)).getModel();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void trainModel(String trainDir) {
		File directory = new File(trainDir);
		File[] fList = directory.listFiles();
		String modelFileName = directory.getParent()+ "/Model.txt";
		log.info("Training Model...");

		for (File trainFile : fList) {

			try {
				FileReader datafr = new FileReader(trainFile);
				EventStream es;
				es = new MaxEntEventStream(
						new PlainTextByLineDataStream(datafr));

				model = GIS.trainModel(100, new OnePassDataIndexer(es, 0),
						false);

				File outputFile = new File(modelFileName);
				GISModelWriter writer = new SuffixSensitiveGISModelWriter(
						(AbstractModel) model, outputFile);
				writer.persist();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		log.info("Train Model FINISHED");
	}

	public void testModel(String testDir) {
		File directory = new File(testDir);
		File[] fList = directory.listFiles();
		MaxEnThread[] threadGroup = new MaxEnThread[fList.length];
		int n = 0;
		for (File file : fList) {
			if (file.isFile()) {
				threadGroup[n] = new MaxEnThread(this, file);
				n++;
			}
		}

		log.info("Test Model...");
		for (MaxEnThread thread : threadGroup) {
			thread.start();
		}

		long testCase = 0;
		long goodCase = 0;
		long badCase = 0;
		for (MaxEnThread thread : threadGroup) {
			try {
				thread.join();
				testCase += thread.testCase();
				goodCase += thread.goodCase();
				badCase += thread.badCase();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		log.info("Model Test Result:\n \tTest Case\t=" + testCase
				+ "\n\tGood Case\t=" + goodCase + "\n\tBad Case\t=" + badCase
				+ "\n");
	}

	public String eval(String document) {
		String[] context = cg.getContext(document);
		double[] outcome = model.eval(context);
		return model.getBestOutcome(outcome);
	}

	public String eval(String[] context) {
		double[] outcome = model.eval(context);
		return model.getBestOutcome(outcome);
	}

	static public void run(String[] args) {
		MaxEnt maxent = new MaxEnt();
		maxent.trainModel(args[0] + "/train/");
		maxent.testModel(args[0] + "/test/");
	}
}
