package com.b5m.maxent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.LinkedList;
import java.util.regex.Pattern;

public class MaxEntTrainer {

    private static final Logger log = LoggerFactory.getLogger(MaxEntTrainer.class);

    // args[0]  SCD Directory
    // args[1]  Model Directory
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: MaxEntTrainer scdDir outDir");
            System.exit(1);
        }

        File scdDir = new File(args[0]);
        File outDir = new File(args[1]);
        File trainDir = new File(outDir, "train");
        File testDir = new File(outDir, "test");

        getData(scdDir, trainDir, testDir);
        MaxEnt.run(trainDir, testDir);
    }

    private static void getData(File scdDir, File trainDir, File testDir) {
        // get SCD files in directory
        File[] fList = scdDir.listFiles();

        // split files: XXX 1/3 for train and 2/3 for test
        final int trainFiles = (int) (fList.length * 0.3) + 1;
        int trainedFiles = 0;

        // assign processing to threads
        // XXX each file processed by one thread!
        List<Thread> threadGroup = new LinkedList<Thread>();

        for (File file : fList) {
            if (!file.isFile()) continue;

            if (trainedFiles < trainFiles) {
                threadGroup.add(new Thread(new MaxEntDataExtractor(file, trainDir)));
            }
            else {
                threadGroup.add(new Thread(new MaxEntDataExtractor(file, testDir)));
            }
            trainedFiles++;
        }

        // TODO use java.util.concurrent
        log.info("Extracting Training Data and Test Data from SCD Files...");
        for (Thread thread : threadGroup) {
            thread.start();
        }

        for (Thread thread : threadGroup) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }
        log.info("Extracting Training Data and Test Data from SCD Files finished");
    }

}
