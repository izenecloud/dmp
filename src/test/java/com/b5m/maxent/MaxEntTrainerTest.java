package com.b5m.maxent;

import com.b5m.utils.Files;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class MaxEntTrainerTest {

    private final File scdDir;
    private final File outDir;

    public MaxEntTrainerTest() throws IOException {
        File scd = Files.getResource("/B-00-201312091124-16026-U-C.SCD");
        scdDir = scd.getParentFile();
        outDir = Files.tempDir("MaxEntTrainerTest", false);
    }

    @Test
    public void test() throws Exception {
        MaxEntTrainer trainer = new MaxEntTrainer(scdDir, outDir);

        File model = trainer.train();
        assertTrue(model.exists() && model.isFile());

        TrainResults results = trainer.test();
        //assertTrue(results.goodCases + results.badCases > 0);
    }

}
