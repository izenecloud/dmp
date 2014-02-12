package com.b5m.maxent;

import com.b5m.scd.DataExtractor;
import com.b5m.scd.ScdFileFilter;
import com.b5m.executors.Shutdown;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.model.AbstractModel;
import opennlp.model.Event;
import opennlp.model.EventStream;
import opennlp.model.MaxentModel;
import opennlp.maxent.GIS;
import opennlp.maxent.io.GISModelWriter;
import opennlp.maxent.io.SuffixSensitiveGISModelWriter;
import opennlp.model.OnePassDataIndexer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

final class MaxEntTrainer {

    private final static Logger log = LoggerFactory.getLogger(MaxEntTrainer.class);

    private final static float TRAIN_TEST_RATE   = 0.6f;
    private final static int POOL_SIZE           = 4;
    private final static int TRAIN_ITERATIONS    = 100;
    private final static boolean TRAIN_SMOOTHING = false;

    private final File scdDir;
    private final File modelFile;

    private File tempDir;
    private File trainDir;
    private File testDir;

    private List<File> trainFiles;
    private List<File> testFiles;

    MaxEntTrainer(File scdDir, File modelFile) {
        this.scdDir = scdDir;
        this.modelFile = modelFile;
    }

    /**
     * Train a MaxEnt classifier using files in directory and save into model.
     */
    void train() throws IOException {
        createDirectories();
        splitFileList(TRAIN_TEST_RATE);
        extractData();
        doTrain();
    }

    /**
     * Test a MaxEnt classifier using model agains files in directory.
     */
    TrainResults test() throws IOException, ExecutionException {
        return doTest();
    }

    File getTempDir() {
        return tempDir;
    }

    private void createDirectories() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "MaxEntTrainer"+System.currentTimeMillis());
        tempDir.mkdir();

        trainDir = new File(tempDir, "train");
        trainDir.mkdir();

        testDir = new File(tempDir, "test");
        testDir.mkdir();
    }

    private void splitFileList(float rate) {
        List<File> files = ScdFileFilter.scdFilesIn(scdDir);
        if (log.isDebugEnabled()) for (File file : files) log.debug("found file: " + file);
        log.info("Found {} files in {}", files.size(), scdDir);

        int index = (int) (files.size() * rate);
        trainFiles = files.subList(0, index);
        testFiles = files.subList(index, files.size());
        log.info("Using #train = {} and #test = {}", trainFiles.size(), testFiles.size());
    }

    private void extractData() {
        log.info("Extracting data from SCD Files...");

        int numThreads = Math.min(POOL_SIZE, trainFiles.size());
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        for (File file : trainFiles) {
            pool.submit(new DataExtractor(file, trainDir));
        }
        for (File file : testFiles) {
            pool.submit(new DataExtractor(file, testDir));
        }

        Shutdown.andWait(pool, 60, TimeUnit.SECONDS);
        log.info("Title-Category pairs extracted from SCD files");
    }

    private void doTrain() throws IOException {
        log.info("Training model ...");

        List<File> files = Arrays.asList(
                trainDir.listFiles(new OutFileFilter()));
        log.info("Found {} train files in {}", files.size(), trainDir);

        EventStream eventStream = new FilesEventStream(files,
                DataExtractor.SEPARATOR);

        MaxentModel model = GIS.trainModel(
                TRAIN_ITERATIONS,
                new OnePassDataIndexer(eventStream),
                TRAIN_SMOOTHING);

        GISModelWriter writer = new SuffixSensitiveGISModelWriter(
                (AbstractModel) model,
                modelFile);
        writer.persist();

        log.info("Model written to file: {}", modelFile);
    }

    TrainResults doTest() throws IOException, ExecutionException {
        log.info("Testing model file: {} ...", modelFile);

        List<File> files = Arrays.asList(
                testDir.listFiles(new OutFileFilter()));
        log.info("Found {} test files in {}", files.size(), testDir);


        int numThreads = Math.min(POOL_SIZE, files.size());
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        // TODO move this into the task
        MaxEntCategoryClassifier maxent = new MaxEntCategoryClassifier(modelFile); // is this thread-safe?

        List<Future<TrainResults>> results = new LinkedList<Future<TrainResults>>();
        for (File file : files) {
            results.add(pool.submit(new MaxentTestTask(maxent, file)));
        }
        Shutdown.andWait(pool, 60, TimeUnit.SECONDS);

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

        log.info("Test Model FINISHED");
        return trainResults;
    }

}

// TODO rename to TrainStats
class TrainResults {
    long goodCases = 0L;
    long badCases  = 0L;

    @Override
    public String toString() {
        return String.format("#good=%d, #bad=%d", goodCases, badCases);
    }
}

class MaxentTestTask implements Callable<TrainResults> {
    private final MaxEntCategoryClassifier model;
    private final File file;

    private TrainResults results = new TrainResults();

    MaxentTestTask(MaxEntCategoryClassifier maxent, File testFile) {
        model = maxent;
        file = testFile;
    }

    @Override
    public TrainResults call() throws IOException {
        FileReader fr = new FileReader(file);
        EventStream es = new TitleCategoryEventStream(fr,
                Character.toString(DataExtractor.SEPARATOR));

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


