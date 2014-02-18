package com.b5m.pig;

import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;
import org.apache.pig.pigunit.PigTest;

import java.io.File;
import org.testng.annotations.BeforeTest;

@Test(groups={"pig"})
public class LogAnalysisOnPig {

    private String[] expected;

    @BeforeTest
    private void getExpected() throws Exception {
        File file = new File("src/test/data/log_analysis.output");
        expected = FileUtils.readLines(file).toArray(new String[]{});
    }

    @Test
    public void test() throws Exception {
        String[] args = {
            "src/test/properties/log_analysis.properties"
        };

        PigTest test = new PigTest("src/main/pig/log_analysis.pig", null, args);

        test.assertOutput("data5", expected);
    }

}

