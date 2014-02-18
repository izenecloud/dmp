package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.utils.Record;

import com.couchbase.client.CouchbaseClient;

import org.apache.commons.io.FileUtils;
import org.apache.pig.backend.executionengine.ExecJob;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups={"couchbase","pig"})
public class UserCategoriesIT {

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private final static String DATE = "2014-01-21";

    private CouchbaseClient client;
    private List<Record> expectedOneDay = new ArrayList<Record>();
    private List<Record> expectedMultiDays = new ArrayList<Record>();

    @BeforeTest
    public void connect() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
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
        UserCategories job = new UserCategories(DATE, 1);
        job.loadProperties("src/test/properties/user_categories.properties");

        ExecJob results = job.call();
        check(results);

        for (Record record : expectedOneDay) check(record);
    }

    @Test
    public void multiDays() throws Exception {
        UserCategories job = new UserCategories(DATE, 3);
        job.loadProperties("src/test/properties/user_categories.properties");

        ExecJob results = job.call();
        check(results);

        for (Record record : expectedMultiDays) check(record);
    }

    private void check(ExecJob result) {
        assertEquals(result.getStatus(), ExecJob.JOB_STATUS.COMPLETED);
    }

    private void check(Record record) throws Exception {
        String key = String.format("%s::%s", record.getUuid(), record.getDate());

        String json = (String) client.get(key);
        assertNotNull(json);

        Record retrieved = Record.fromJson(json);
        assertEquals(retrieved, record);

        assertTrue(client.delete(key).get());
    }

}

