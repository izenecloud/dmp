package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.utils.Record;

import com.couchbase.client.CouchbaseClient;

import org.apache.commons.io.FileUtils;
import org.apache.pig.pigunit.PigTest;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Test(groups={"couchbase","pig"})
public class UserCategoriesIT {

    private final static String DATE = "2014-01-21";

    private final List<Record> expectedOneDay = new ArrayList<Record>();
    private final List<Record> expectedMultiDays = new ArrayList<Record>();

    private CouchbaseClient client;

    @BeforeTest
    public void connect() throws Exception {
        Properties props = new Properties();
        props.load(new FileReader("src/test/properties/user_categories.properties"));
        List<URI> hosts = Arrays.asList(new URI(props.getProperty("hosts")));
        client = new CouchbaseClient(hosts,
                props.getProperty("bucket"),
                props.getProperty("password").replaceAll("\"", ""));
    }

    @AfterTest
    public void shutdown() {
        client.shutdown(1, TimeUnit.SECONDS);
    }

    @BeforeTest
    public void getExpected() throws Exception {
        getRecords("src/test/data/user_categories_one.output", expectedOneDay);
        getRecords("src/test/data/user_categories_multi.output", expectedMultiDays);
    }

    private void getRecords(String file, List<Record> list) throws Exception {
        List<String> lines = FileUtils.readLines(new File(file));
        for (String line : lines) {
            list.add(Record.fromPig(line));
        }
    }

    @Test
    public void oneDay() throws Exception {
        String[] args = {
            "date=" + DATE,
            "count=1",
        };
        
        String[] params = {
            "src/test/properties/user_categories.properties"
        };

        PigTest pig = new PigTest("src/main/pig/user_categories.pig", args, params);
        pig.unoverride("STORE");
        pig.runScript();

        check(expectedOneDay);
    }

    @Test
    public void multiDays() throws Exception {
        String[] args = {
            "date=" + DATE,
            "count=3",
        };
        
        String[] params = {
            "src/test/properties/user_categories.properties"
        };

        PigTest pig = new PigTest("src/main/pig/user_categories.pig", args, params);
        pig.unoverride("STORE");
        pig.runScript();

        check(expectedMultiDays);
    }

    private void check(List<Record> expected) throws Exception {
        for (Record record : expected) {
            String key = String.format("%s::%s", record.getUuid(), record.getDate());

            String json = (String) client.get(key);
            assertNotNull(json);

            Record retrieved = Record.fromJson(json);
            assertEquals(retrieved, record);

            assertTrue(client.delete(key).get());
        }
    }

}

