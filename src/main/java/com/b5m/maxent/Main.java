package com.b5m.maxent;

import java.io.File;

/**
 * Application entry point for training a MaxEnt model.
 *
 * @author Paolo D'Apice
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: MaxEntTrainer scdDir modelFile");
            System.exit(1);
        }

        File scdDir = new File(args[0]);
        if (!scdDir.isDirectory()) {
            System.out.printf("%s is not a directory\n", args[0]);
            System.exit(2);
        }

        File modelFile = new File(args[1]);
        MaxEntTrainer trainer = new MaxEntTrainer(scdDir, modelFile);

        trainer.train();
        TrainResults results = trainer.test();

        System.out.println("Model Test Results: " + results);
    }

}

