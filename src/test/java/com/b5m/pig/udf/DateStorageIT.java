package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;

@Test(groups={"pig"})
public class DateStorageIT {

    private Cluster cluster;
    private PigTest test;

    @BeforeTest
    public void setup() throws Exception {
        cluster = PigTest.getCluster();
    }

    @Test
    public void store() throws Exception {
        String[] args = {
            "input=src/test/data/pig-output.txt",
            "output=dmp",
            "date=2014-02-21",
        };

        test = new PigTest("src/test/pig/date_storage_store.pig", args);

        test.unoverride("STORE");
        test.runScript();
        
        Path outputDir = new Path("dmp/2014/02/21");
        assertTrue(cluster.exists(outputDir));

        assertTrue(cluster.delete(new Path("dmp")));
    }

    @Test
    public void load() throws Exception {
        String[] args = {
            "input=src/test/data",
            "date=2014-01-21",
            "count=4"
        };

        test = new PigTest("src/test/pig/date_storage_load.pig", args);

        String[] expected = {
            "(048bc3df5411e3b81b42796f4ffe0da8,[图书音像#2])",
            "(0f2ec03f725c3348975b06f0259370e2,[图书音像#1,家用电器#1])",
            "(00cfec9a06a584b178e020b9698c8fbb,[服装服饰#1])",
            "(0719533fae4ae7aa74058543c9dfd310,[图书音像#1,服装服饰#1])",
            "(069cb38a3779a75c7bfa6f91fb732573,[鞋包配饰#1])",
            "(0719533fae4ae7aa74058543c9dfd310,[图书音像#1])",
        };

        test.assertOutput("data", expected);
    }

}
