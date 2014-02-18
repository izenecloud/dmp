package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;

import java.io.File;
import org.apache.commons.io.FileUtils;

@Test(groups={"pig"})
public class MaxEntClassifierBuilderOnPig {
    
    @Test
    public void train() throws Exception{
        File model = new File("target/Model.txt");
        model.deleteOnExit();

        String[] args = {
            "input=src/test/data/maxent",
            "output=" + model,
        };

        PigTest test = new PigTest("src/test/pig/maxent_train.pig", args);
        test.unoverride("STORE");
        test.runScript();

        assertTrue(model.isFile());
        assertTrue(FileUtils.contentEquals(model, new File("src/test/data/Model.out")));
    }
    
}
