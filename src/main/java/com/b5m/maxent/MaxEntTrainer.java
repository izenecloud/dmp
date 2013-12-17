package com.b5m.maxent;

import com.b5m.maxent.MaxEntDataExtractor.MaxEntThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class MaxEntTrainer {

    private static final Logger log = LoggerFactory.getLogger(MaxEntTrainer.class);

    // args[0]  SCD Directory
    // args[1]  Model Directory
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: MaxEntTrainer scdDir outDir");
            System.exit(1);
        }

        String scdDir = args[0];
        String outDir = args[1];
        String trainDir = outDir + "/train/"; // FIXME use File
        String testDir = outDir + "/test/";   // FIXME use File

        getData(scdDir, trainDir, testDir);
        MaxEnt.run(trainDir, testDir);
    }

    // FIXME pass File not String
    private static void getData(String scdDir, String trainDir, String testDir) {
        File directory = new File(scdDir);

        File[] fList = directory.listFiles();
        Thread[] threadGroup = new Thread[fList.length];

        // split files: 1/3 for train and 2/3 for test
        int trainFiles = (int) (fList.length * 0.3) + 1;
        int trainedFiles = 0;
        for (File file : fList) {
            if (file.isFile()) {
                if (trainedFiles < trainFiles) {
                    threadGroup[trainedFiles] = new Thread(new MaxEntThread(file.getAbsolutePath(), trainDir));
                }
                else {
                    threadGroup[trainedFiles] = new Thread(new MaxEntThread(file.getAbsolutePath(), testDir));
                }
                trainedFiles++;
            }
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
