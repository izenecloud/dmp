package com.b5m.maxent;

import com.b5m.scd.DataExtractor;
import com.b5m.scd.ScdFileFilter;
import com.b5m.executors.Shutdown;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private final static Log log = LogFactory.getLog(MaxEntTrainer.class);

    private final static float FOLD_RATE         = 0.9f;
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
        splitFileList(FOLD_RATE);
        extractData();
        doTrain();
    }

    /**
     * Test a MaxEnt classifier using model agains files in directory.
     */
    TestResults test() throws IOException, ExecutionException {
        return doTest();
    }

    File getTempDir() {
        return tempDir;
    }

    private void createDirectories() {
        tempDir = new File(System.getProperty("java.io.tmpdir"),
                           "MaxEntTrainer" + System.currentTimeMillis());
        tempDir.mkdir();

        trainDir = new File(tempDir, "train");
        trainDir.mkdir();

        testDir = new File(tempDir, "test");
        testDir.mkdir();
    }

    private void splitFileList(float rate) {
        List<File> files = ScdFileFilter.scdFilesIn(scdDir);
        if (log.isDebugEnabled()) for (File file : files) log.debug("found file: " + file);

        if (log.isInfoEnabled())
            log.info(String.format("Found %d files in %s", files.size(), scdDir));

        int index = (int) (files.size() * rate);
        trainFiles = files.subList(0, index);
        testFiles = files.subList(index, files.size());

        if (log.isInfoEnabled())
            log.info(String.format("Using #train = %s and #test = %s",
                                   trainFiles.size(), testFiles.size()));
    }

    private void extractData() {
        if (log.isInfoEnabled())
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

        if (log.isInfoEnabled())
            log.info("Title-Category pairs extracted from SCD files");
    }

    private void doTrain() throws IOException {
        if (log.isInfoEnabled())
            log.info("Training model ...");

        List<File> files = Arrays.asList(
                trainDir.listFiles(new OutFileFilter()));

        if (log.isInfoEnabled())
            log.info(String.format("Found %d train files in %s", files.size(), trainDir));

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

        if (log.isInfoEnabled())
            log.info("Model written to file: " + modelFile);
    }

    TestResults doTest() throws IOException, ExecutionException {
        if (log.isInfoEnabled())
            log.info("Testing model file: " + modelFile);

        List<File> files = Arrays.asList(
                testDir.listFiles(new OutFileFilter()));

        if (log.isInfoEnabled())
            log.info(String.format("Found %d test files in %s", files.size(), testDir));

        int numThreads = Math.min(POOL_SIZE, files.size());
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        List<Future<TestResults>> futures = new LinkedList<Future<TestResults>>();
        for (File file : files) {
            futures.add(pool.submit(new MaxentTestTask(file)));
        }
        Shutdown.andWait(pool, 60, TimeUnit.SECONDS);

        TestResults results = new TestResults();
        for (Future<TestResults> future : futures) {
            try {
                TestResults res = future.get();

                results.goodCases += res.goodCases;
                results.badCases  += res.badCases;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (log.isInfoEnabled())
            log.info("Testing model finished: " + results);

        return results;
    }

    private class MaxentTestTask implements Callable<TestResults> {
        private final MaxEntCategoryClassifier model;
        private final File file;

        MaxentTestTask(File testFile) throws IOException {
            model = new MaxEntCategoryClassifier(modelFile);
            file = testFile;
        }

        @Override
        public TestResults call() throws IOException {
            FileReader fr = new FileReader(file);
            EventStream es = new TitleCategoryEventStream(fr,
                    Character.toString(DataExtractor.SEPARATOR));

            TestResults results = new TestResults();

            while (es.hasNext()) {
                Event event = es.next();
                String expected = event.getOutcome();
                String actual = model.eval(event.getContext());
                if (expected.equalsIgnoreCase(actual)) {
                    results.goodCases++;
                } else {
                    results.badCases++;
                }
            }
            return results;
        }
    }

}

class TestResults {
    long goodCases = 0L;
    long badCases  = 0L;

    @Override
    public String toString() {
        return String.format("#good=%d, #bad=%d", goodCases, badCases);
    }
}

