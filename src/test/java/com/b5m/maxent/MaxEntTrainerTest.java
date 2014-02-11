package com.b5m.maxent;

import com.b5m.utils.Files;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;

import java.io.File;

@Test(groups={"maxent"})
public class MaxEntTrainerTest {

    private File scdDir;
    private File outDir;
    private File expected;

    @BeforeTest()
    public void setup() throws Exception {
        File scd = new File("src/test/data/test.scd");
        scdDir = scd.getParentFile();
        outDir = Files.tempDir("MaxEntTrainerTest");
        expected = new File("src/test/data/Model.out");
    }

    @AfterTest
    public void cleanup() throws Exception {
        FileUtils.deleteDirectory(outDir);
    }

    @Test
    public void test() throws Exception {
        MaxEntTrainer trainer = new MaxEntTrainer(scdDir, outDir);

        File model = trainer.train();
        assertTrue(model.isFile());
        assertTrue(FileUtils.contentEquals(model, expected));

        TrainResults results = trainer.test();
        //assertTrue(results.goodCases + results.badCases > 0);
    }

}
