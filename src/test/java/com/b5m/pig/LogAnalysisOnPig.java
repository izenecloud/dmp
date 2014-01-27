package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;
import org.apache.pig.pigunit.PigTest;

import java.io.File;
import java.util.Date;
import java.util.List;

public class LogAnalysisOnPig {

    private static String[] expected;

    @BeforeClass
    private static void getExpected() throws Exception {
        List<String> lines = FileUtils.readLines(new File("src/test/data/log_analysis.output"));
        expected = lines.toArray(new String[lines.size()]);
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

