package com.b5m.pig.udf;

import com.b5m.pig.Record;

import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;
import org.apache.commons.io.FileUtils;

import com.couchbase.client.CouchbaseClient;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CouchbaseStorageOnPig {

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private static CouchbaseClient client;
    private static List<String> expected;

    @BeforeClass
    public static void connect() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
    }

    @BeforeClass
    private static void getExpected() throws Exception {
        expected = FileUtils.readLines(new File("src/test/data/couchbase-expected.txt"));
    }

    @Test
    public void test() throws Exception {
        String[] args = {
            "hosts=" + HOST,
            "bucket=" + BUCKET,
            "input=src/test/data/pig-output.txt",
        };

        PigTest test = new PigTest("src/test/pig/couchbaseStorage.pig", args);

        test.unoverride("STORE"); // enables write to couchbase
        test.runScript();

        for (String json : expected) {
            String key = Record.fromJson(json).getUuid();
            assertEquals(client.get(key), json);
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        for (String json : expected) {
            String key = Record.fromJson(json).getUuid();
            assertTrue(client.delete(key).get());
        }
        client.shutdown();
    }

}

