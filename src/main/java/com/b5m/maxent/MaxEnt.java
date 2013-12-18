package com.b5m.maxent;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

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

public class MaxEnt implements CategoryClassifier {

    private static final Logger log = LoggerFactory.getLogger(MaxEnt.class);

    public static final String MODEL_FILENAME = "Model.txt";

    private MaxentModel model;
    private ContextGenerator cg = new MaxEntContextGenerator();

    public MaxEnt(File modelFile) throws IOException {
        log.info("Loading model from: " +  modelFile);
        model = new GenericModelReader(modelFile).getModel();
    }

    public String getCategory(String title) {
        String[] context = cg.getContext(title);
        return eval(context);
    }

    String eval(String[] context) {
        double[] outcome = model.eval(context);
        return model.getBestOutcome(outcome);
    }

    public static File trainModel(File directory) throws IOException {
        log.info("Training Model...");

        File outputFile = new File(directory, MODEL_FILENAME);

        File[] fList = directory.listFiles();
        for (File trainFile : fList) {
            log.info("input file: " + trainFile);

            FileReader datafr = new FileReader(trainFile);
            EventStream es = new MaxEntEventStream(new PlainTextByLineDataStream(datafr));

            MaxentModel model = GIS.trainModel(100, new OnePassDataIndexer(es, 0), false);

            GISModelWriter writer = new SuffixSensitiveGISModelWriter(
                    (AbstractModel) model, outputFile);
            writer.persist();

            datafr.close();
        }
        log.info("Train Model FINISHED");

        return outputFile;
    }

    public static void testModel(File model, File directory) throws IOException {
        log.info("Testing Model...");

        MaxEnt maxent = new MaxEnt(model); // is this thread-safe?

        File[] fList = directory.listFiles();
        List<MaxEnThread> threadGroup = new LinkedList<MaxEnThread>();
        for (File file : fList) {
            if (file.isFile()) {
                threadGroup.add(new MaxEnThread(maxent, file));
            }
        }

        for (MaxEnThread thread : threadGroup) {
            thread.start();
        }

        long goodCases = 0L;
        long badCases  = 0L;

        for (MaxEnThread thread : threadGroup) {
            try {
                thread.join();

                goodCases += thread.results.goodCases;
                badCases  += thread.results.badCases;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }
        log.info("Train Model FINISHED");

        System.out.println("Model Test Results:");
        System.out.printf("Test Cases: %d\n", goodCases + badCases);
        System.out.printf("      Good: %d\n", goodCases);
        System.out.printf("       Bad: %d\n", badCases);
    }
}

class TrainResults {
    long goodCases = 0L;
    long badCases  = 0L;
}

class MaxEnThread extends Thread {
    private final MaxEnt model;
    private final File file;

    /*private*/ TrainResults results;

    MaxEnThread(MaxEnt maxent, File testFile) {
        model = maxent;
        file = testFile;
    }

    @Override
    public void run() {
        try {
            FileReader datafr = new FileReader(file);
            EventStream es = new MaxEntEventStream(
                    new PlainTextByLineDataStream(datafr));

            while (es.hasNext()) {
                Event event = es.next();
                String outcome = model.eval(event.getContext());
                if (event.getOutcome().equalsIgnoreCase(outcome)) {
                    results.goodCases++;
                } else {
                    results.badCases++;
                }
            }
        } catch (Exception e) {
            //log.error(e.getMessage(), e);
        }
    }
}


