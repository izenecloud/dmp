package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

import java.io.IOException;
import java.io.File;

public class SimpleDmpOnPig {

    @Test(enabled=false) // XXX cannot perform check due to ordering
    public void test() throws IOException, ParseException {
        String[] args = {
            "./src/test/pig/simple-dmp.properties"
        };

        PigTest test = new PigTest("./src/main/pig/simple-dmp.pig", null, args);

        test.assertOutput(new File("./src/test/resources/dmp-output.txt"));
    }

}

