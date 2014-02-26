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
import java.util.concurrent.TimeUnit;

@Test(groups={"couchbase"})
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
    public void usage(String key, String value) throws Exception {
        assertTrue(client.set(key, value).get());
        assertEquals(client.get(key), value);
        assertTrue(client.delete(key).get());
        assertNull(client.get(key));
    }

    @Test
    public void expiration() throws Exception {
        int expire = 1; // seconds
        
        for (Object[] oo: pairs()) {
            String key = (String) oo[0];
            assertTrue(client.set(key, expire, oo[1]).get());
            assertNotNull(client.get(key));
        }

        TimeUnit.SECONDS.sleep(expire + 1);
        
        for (Object[] oo: pairs()) {
            String key = (String) oo[0];
            assertNull(client.get(key));
        }
    }
}
