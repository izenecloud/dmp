package com.b5m.pig;

import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

@Test(groups={"pig"})
public class PigUsageIT {
    
    @Test
    public void maps() throws Exception {
        String[] script = {
            "a = load 'input' as (data:map[int]);",
            "b = foreach a generate data#'key1' as k1, data#'key2' as k2;",
            "store b into 'output';",
        };

        String[] input = {
            "[key1#1,key2#2]",
            "[key1#1,key3#3]",
            "[key2#2,key3#3]",
            "[你好#2,不好#3]",
        }; 
        
        String[] output = {
            "(1,2)",
            "(1,)",
            "(,2)",
            "(,)",
        };

        PigTest test = new PigTest(script);
        test.assertOutput("a", input, "b", output);
    }
    
}
