package com.b5m.couchbase;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.couchbase.client.CouchbaseClient;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class CouchbaseClientIT {

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private CouchbaseClient client;

    @BeforeTest
    public void setup() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
    }

    @AfterTest
    public void teardown() {
        client.shutdown();
    }

    @DataProvider
    public Object[][] pairs() {
        return new Object[][] {
            { "test-document", "hello couchbase" },
            { "测试文件","你好couchbase" },
        };
    }

    @Test(dataProvider="pairs")
    public void simple(String key, String value) throws Exception {
        assertNull(client.get(key));

        client.set(key, value).get();

        String retrieved = (String) client.get(key);
        assertEquals(retrieved, value);

        boolean deleted = client.delete(key).get();
        assertTrue(deleted);
    }
}
