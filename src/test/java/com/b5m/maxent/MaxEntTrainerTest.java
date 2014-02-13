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
    private File modelFile;
    private File expected;

    private File tempdir;

    @BeforeTest()
    public void setup() throws Exception {
        scdDir = new File("src/test/data/maxent");
        modelFile = new File(Files.systemTmpDir(), "Model.txt");
        expected = new File("src/test/data/Model.out");
    }

    @AfterTest
    public void cleanup() throws Exception {
        FileUtils.deleteDirectory(tempdir);
        modelFile.delete();
    }

    @Test
    public void test() throws Exception {
        MaxEntTrainer trainer = new MaxEntTrainer(scdDir, modelFile);

        trainer.train();
        assertTrue(modelFile.isFile());
        assertTrue(FileUtils.contentEquals(modelFile, expected));

        TestResults results = trainer.test();
        assertTrue(results.goodCases >= 0);
        assertTrue(results.badCases >= 0);
        assertTrue(results.goodCases + results.badCases > 0);

        tempdir = trainer.getTempDir();
    }

}
