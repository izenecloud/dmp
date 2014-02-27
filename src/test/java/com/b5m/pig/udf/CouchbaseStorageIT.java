package com.b5m.pig.udf;

import com.b5m.utils.Record;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;
import org.apache.commons.io.FileUtils;

import com.couchbase.client.CouchbaseClient;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups={"couchbase","pig"})
public class CouchbaseStorageIT {

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private CouchbaseClient client;
    private List<String> expected;

    @BeforeTest
    public void connect() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
    }

    @AfterTest
    public void shutdown() {
        client.shutdown();
    }

    @BeforeTest
    public void getExpected() throws Exception {
        File file = new File("src/test/data/couchbase-expected.txt");
        expected = FileUtils.readLines(file);
    }

    @Test
    public void store() throws Exception {
        String[] args = {
            "hosts=" + HOST,
            "bucket=" + BUCKET,
            "input=src/test/data/pig-output.txt",
        };

        PigTest test = new PigTest("src/test/pig/couchbase_storage.pig", args);

        test.unoverride("STORE"); // enables write to couchbase
        test.runScript();

        for (String json : expected) {
            String key = Record.fromJson(json).getUuid();
            assertEquals(client.get(key), json);

            assertTrue(client.delete(key).get());
        }
    }

    @Test
    public void expire() throws Exception {
        int expire = 1;
        String[] args = {
            "hosts=" + HOST,
            "bucket=" + BUCKET,
            "input=src/test/data/pig-output.txt",
            "expire=" + expire,
        };

        PigTest test = new PigTest("src/test/pig/couchbase_storage.pig", args);

        test.unoverride("STORE"); // enables write to couchbase
        test.runScript();

        TimeUnit.SECONDS.sleep(expire + 1);

        for (String json : expected) {
            String key = Record.fromJson(json).getUuid();
            assertNull(client.get(key));
        }
    }

}

