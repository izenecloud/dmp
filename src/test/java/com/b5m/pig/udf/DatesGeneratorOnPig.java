package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;

import java.util.Date;

public class DatesGeneratorOnPig {

    @Test
    public void single() throws Exception {
        String[] script = {
            "REGISTER dist/pig-udfs.jar",
            "DEFINE DatesGenerator com.b5m.pig.udf.DatesGenerator('20140110');",
            "dates = LOAD 'dummy' USING DatesGenerator AS (date:chararray);",
            "STORE dates INTO 'output';",
        };

        PigTest test = new PigTest(script);

        String[] expected = {
            "(20140110)",
        };

        test.assertOutput("dates", expected);
    }

    @Test
    public void multi() throws Exception {
        String[] script = {
            "REGISTER dist/pig-udfs.jar",
            "DEFINE DatesGenerator com.b5m.pig.udf.DatesGenerator('20140110','5');",
            "dates = LOAD 'unused' USING DatesGenerator AS (date:chararray);",
            "STORE dates INTO 'output';",
        };

        PigTest test = new PigTest(script);

        String[] expected = {
            "(20140110)",
            "(20140109)",
            "(20140108)",
            "(20140107)",
            "(20140106)",
        };

        test.assertOutput("dates", expected);
    }


}

