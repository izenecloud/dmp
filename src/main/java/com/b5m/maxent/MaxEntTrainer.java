package com.b5m.maxent;

import com.b5m.executors.Shutdown;
import com.b5m.scd.DataExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MaxEntTrainer {

    private static final Logger log = LoggerFactory.getLogger(MaxEntTrainer.class);

    private final File scdDir;
    private final File trainDir;
    private final File testDir;

    private File model;

    MaxEntTrainer(File scdDir, File outDir) {
        this.scdDir = scdDir;
        trainDir = new File(outDir, "train");
        testDir = new File(outDir, "test");
    }

    File train() throws IOException {
        trainDir.mkdir();
        testDir.mkdir();

        if (!testDir.isDirectory() || !trainDir.isDirectory()) {
            throw new IOException("Cannot create output directories");
        }

        // extract data from SCD files
        getData();

        model = MaxEnt.trainModel(trainDir);
        return model;
    }

    TrainResults test() throws IOException, ExecutionException {
        return MaxEnt.testModel(model, testDir);
    }

    private void getData() {
        log.info("Getting data from SCD ...");

        // get SCD files in directory
        File[] fList = scdDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile() && file.getName().matches("^.+\\.(SCD|scd)$");
            }
        });

        log.info("Found {} files in {}", fList.length, scdDir);
        if (log.isDebugEnabled()) {
            for (File file : fList)
                log.debug("found file: " + file);
        }

        // split files: XXX 1/3 for train and 2/3 for test
        final int trainFiles = (int) (fList.length * 0.3) + 1;
        int trainedFiles = 0;

        // submit jobs to thread pool
        ExecutorService pool = Executors.newCachedThreadPool();

        log.info("Extracting Training Data and Test Data from SCD Files...");
        for (File file : fList) {
            if (trainedFiles < trainFiles) {
                pool.submit(new DataExtractor(file, trainDir));
            }
            else {
                pool.submit(new DataExtractor(file, testDir));
            }
            trainedFiles++;
        }

        Shutdown.andWait(pool, 60, TimeUnit.SECONDS);

        log.info("SCD data extracted");
    }

    public static void main(String[] args) throws IOException, ExecutionException {
        if (args.length < 2) {
            System.out.println("usage: MaxEntTrainer scdDir outDir");
            System.exit(1);
        }

        File scdDir = new File(args[0]);
        if (!scdDir.exists() || !scdDir.isDirectory()) {
            System.out.printf("%s is not a directory\n", args[0]);
            System.exit(2);
        }

        File outDir = new File(args[1]);
        if (!outDir.exists() || !outDir.isDirectory()) {
            System.out.printf("%s is not a directory\n", args[1]);
            System.exit(2);
        }

        MaxEntTrainer trainer = new MaxEntTrainer(scdDir, outDir);
        File model = trainer.train();
        TrainResults results = trainer.test();

        System.out.println("Model Test Results:");
        System.out.printf("Test Cases: %d\n", results.goodCases + results.badCases);
        System.out.printf("      Good: %d\n", results.goodCases);
        System.out.printf("       Bad: %d\n", results.badCases);
    }

}

