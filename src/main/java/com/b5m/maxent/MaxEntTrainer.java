package com.b5m.maxent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class MaxEntTrainer {

    private static final Logger log = LoggerFactory.getLogger(MaxEntTrainer.class);

    // args[0]  SCD Directory
    // args[1]  Model Directory
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("usage: MaxEntTrainer scdDir outDir");
            System.exit(1);
        }

        File scdDir = new File(args[0]);
        File outDir = new File(args[1]);
        File trainDir = new File(outDir, "train");
        File testDir = new File(outDir, "test");

        getData(scdDir, trainDir, testDir);

        File model = MaxEnt.trainModel(trainDir);
        MaxEnt.testModel(model, testDir);
    }

    private static void getData(File scdDir, File trainDir, File testDir) {
        // get SCD files in directory
        File[] fList = scdDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.matches("^.\\.(SCD|scd)^");
            }
        });

        // split files: XXX 1/3 for train and 2/3 for test
        final int trainFiles = (int) (fList.length * 0.3) + 1;
        int trainedFiles = 0;

        // submit jobs to thread pool
        ExecutorService executor = Executors.newCachedThreadPool();

        log.info("Extracting Training Data and Test Data from SCD Files...");
        for (File file : fList) {
            if (!file.isFile()) continue;

            if (trainedFiles < trainFiles) {
                executor.submit(new MaxEntDataExtractor(file, trainDir));
            }
            else {
                executor.submit(new MaxEntDataExtractor(file, testDir));
            }
            trainedFiles++;
        }

        executor.shutdown();
        log.info("Extracting Training Data and Test Data from SCD Files finished");
    }

}
