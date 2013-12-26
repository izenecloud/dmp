package com.b5m.couchbase;

import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.couchbase.client.CouchbaseClient;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

@Test(enabled=false) // it requires a running Couchbase server
public class TestCouchbaseClient {

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private static CouchbaseClient client;

    @BeforeClass
    public static void setup() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
    }

    @AfterClass
    public static void teardown() {
        client.shutdown();
    }

    public void simple() throws Exception {
        String key = "test-document";
        String val = "hello couchbase";

        assertNull(client.get(key));

        client.set(key, val).get();

        String retrieved = (String) client.get(key);
        assertEquals(retrieved, val);

        boolean deleted = client.delete(key).get();
        assertTrue(deleted);
    }
}
