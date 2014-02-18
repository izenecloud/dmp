package com.b5m.pig.udf;

import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

@Test(groups={"pig","maxent"})
public class MaxEntPairsOnPig {

    @Test
    public void pairs() throws Exception {
        String[] args = {
            "input=src/test/data/test.scd",
        };

        String[] expected = {
            "(39)"
        };
        
        PigTest test = new PigTest("src/test/pig/maxent_pairs.pig", args);
        test.assertOutput("count", expected);
    }

}
