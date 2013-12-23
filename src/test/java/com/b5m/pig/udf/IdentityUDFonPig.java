package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

import java.io.IOException;

public class IdentityUDFonPig {

    @Test(enabled=false)
    public void testIdentityUDF() throws IOException, ParseException {
        PigTest test = new PigTest("./src/test/pig/identityUdf.pig");

        String[] input = {
                "hello",
                "hello"
        };

        String[] output = { // TODO understand pig output format
                "((hello))",
                "((hello))",
        };

        test.assertOutput(
                "data", input,
                "data2",
        output);
    }

}

