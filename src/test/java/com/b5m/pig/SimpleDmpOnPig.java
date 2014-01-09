package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;
import org.apache.pig.pigunit.PigTest;

import java.io.IOException;
import java.io.File;
import java.util.List;

public class SimpleDmpOnPig {

    private static String[] expected;

    @BeforeClass
    private static void getExpected() throws Exception {
        List<String> lines = FileUtils.readLines(new File("src/test/data/simple-dmp.output"));
        expected = lines.toArray(new String[lines.size()]);
    }

    @Test
    public void test() throws Exception {
        String[] args = {
            "src/test/pig/simple-dmp.properties"
        };

        PigTest test = new PigTest("src/main/pig/simple-dmp.pig", null, args);

        test.assertOutput("data5", expected);
    }

}

