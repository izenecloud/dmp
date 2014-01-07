package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class SimpleDmpOnPig {

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private static CouchbaseClient client;
    private static Set<String> uuids;

    @BeforeClass
    public static void connect() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
    }

    @BeforeClass
    private static void getExpected() throws Exception {
        List<String> lines = FileUtils.readLines(new File("src/test/data/uuid.txt"));
        uuids = new TreeSet<String>(lines);
    }
    @Test
    public void test() throws IOException, ParseException {
        String[] args = {
            "./src/test/pig/simple-dmp.properties"
        };

        PigTest test = new PigTest("./src/main/pig/simple-dmp.pig", null, args);

        test.unoverride("STORE"); // enables write to couchbase
        test.runScript();

        for (String uuid : uuids) {
            assertNotNull(client.get(uuid));
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        for (String uuid : uuids) {
            assertTrue(client.delete(uuid).get());
        }
        client.shutdown();
    }
}

