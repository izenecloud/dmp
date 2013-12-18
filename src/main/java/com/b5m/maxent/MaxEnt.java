package com.b5m.maxent;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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

    public static void testModel(File model, File directory) throws IOException, ExecutionException {
        log.info("Testing Model...");

        MaxEnt maxent = new MaxEnt(model); // is this thread-safe?

        // submit jobs to thread pool
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<TrainResults>> results = new LinkedList<Future<TrainResults>>();

        File[] fList = directory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile();
            }
        });

        for (File file : fList) {
            results.add(executor.submit(new MaxEnThread(maxent, file)));
        }

        executor.shutdown();

        TrainResults trainResults = new TrainResults();

        for (Future<TrainResults> future : results) {
            try {
                TrainResults res = future.get();

                trainResults.goodCases += res.goodCases;
                trainResults.badCases  += res.badCases;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }
        log.info("Train Model FINISHED");

        System.out.println("Model Test Results:");
        System.out.printf("Test Cases: %d\n", trainResults.goodCases + trainResults.badCases);
        System.out.printf("      Good: %d\n", trainResults.goodCases);
        System.out.printf("       Bad: %d\n", trainResults.badCases);
    }
}

class TrainResults {
    long goodCases = 0L;
    long badCases  = 0L;
}

class MaxEnThread implements Callable<TrainResults> {
    private final MaxEnt model;
    private final File file;

    private TrainResults results = new TrainResults();

    MaxEnThread(MaxEnt maxent, File testFile) {
        model = maxent;
        file = testFile;
    }

    @Override
    public TrainResults call() throws IOException {
        FileReader datafr = new FileReader(file);
        EventStream es = new MaxEntEventStream(new PlainTextByLineDataStream(datafr));

        while (es.hasNext()) {
            Event event = es.next();
            String outcome = model.eval(event.getContext());
            if (event.getOutcome().equalsIgnoreCase(outcome)) {
                results.goodCases++;
            } else {
                results.badCases++;
            }
        }

        return results;
    }
}


