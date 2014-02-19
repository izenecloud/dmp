package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;

import java.io.File;
import org.apache.commons.io.FileUtils;

@Test(groups={"pig"})
public class MaxEntClassifierBuilderIT {

    @Test
    public void train() throws Exception{
        File model = new File("target/Model.txt");
        model.deleteOnExit();

        String[] args = {
            "input=src/test/data/maxent",
            "model_file=" + model,
            "udf_file=dist/pig-udfs.jar",
        };

        PigTest test = new PigTest("src/main/pig/model_training.pig", args);
        test.unoverride("STORE");
        test.runScript();

        assertTrue(model.isFile());
        assertTrue(FileUtils.contentEquals(model, new File("src/test/data/Model.out")));
    }

}
