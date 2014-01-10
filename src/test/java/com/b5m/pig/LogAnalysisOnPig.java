package com.b5m.pig;

import com.b5m.utils.Dates;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;
import org.apache.pig.pigunit.PigTest;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LogAnalysisOnPig {

    private static String[] expected;

    @BeforeClass
    private static void getExpected() throws Exception {
        String date = Dates.toString(new Date());

        List<String> lines = FileUtils.readLines(new File("src/test/data/log_analysis.output"));
        List<String> temp = new ArrayList<String>(lines.size());
        for (String line : lines) {
            int i = line.indexOf(',');

            StringBuilder sb = new StringBuilder(line.substring(0, i));
            sb.append("::").append(date);
            sb.append(line.substring(i));

            temp.add(sb.toString());
        }
        expected = temp.toArray(new String[lines.size()]);
    }

    @Test
    public void test() throws Exception {
        String[] args = {
            "src/test/pig/log_analysis.properties"
        };

        PigTest test = new PigTest("src/main/pig/log_analysis.pig", null, args);

        test.assertOutput("data5", expected);
    }

}

