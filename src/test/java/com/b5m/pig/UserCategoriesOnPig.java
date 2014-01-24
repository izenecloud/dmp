package com.b5m.pig;

import com.b5m.utils.DatesGenerator;

import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.couchbase.client.CouchbaseClient;

import org.apache.commons.io.FileUtils;
import org.apache.pig.backend.executionengine.ExecJob;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UserCategoriesOnPig {

    private final static DatesGenerator dategen = new DatesGenerator();

    private final static String BUCKET = "default";
    private final static String PASSWORD = "";
    private final static String HOST = "http://127.0.0.1:8091/pools";

    private final static String DATE = "2014-01-21";

    private static CouchbaseClient client;
    private static List<Record> expectedOneDay = new ArrayList<Record>();
    private static List<Record> expectedMultiDays = new ArrayList<Record>();

    @BeforeClass
    public static void connect() throws Exception {
        List<URI> hosts = Arrays.asList(new URI(HOST));
        client = new CouchbaseClient(hosts, BUCKET, PASSWORD);
    }

    @AfterClass
    public static void shutdown() {
        client.shutdown(1, TimeUnit.SECONDS);
    }

    @BeforeClass
    private static void getExpectedOneDay() throws Exception {
        List<String> lines = FileUtils.readLines(new File("src/test/data/user_categories_one.output"));
        for (String line : lines) {
            String text = appendDate(line, DATE);
            expectedOneDay.add(Record.fromPig(text));
        }
    }

    @BeforeClass
    private static void getExpectedMultiDays() throws Exception {
        List<String> lines = FileUtils.readLines(new File("src/test/data/user_categories_multi.output"));
        for (String line : lines) {
            String text = appendDate(line, DATE);
            expectedMultiDays.add(Record.fromPig(text));
        }
    }

    private static String appendDate(String line, String date) {
        int i = line.indexOf(',');
        StringBuilder sb = new StringBuilder(line.substring(0, i));
        sb.append("::").append(date);
        sb.append(line.substring(i));
        return sb.toString();
    }

    @Test
    public void oneDay() throws Exception {
        UserCategories job = new UserCategories(DATE, 1);
        job.loadProperties("src/test/pig/user_categories.properties");

        ExecJob results = job.call();
        check(results);

        for (Record record : expectedOneDay) check(record);
    }

    @Test
    public void multiDays() throws Exception {
        UserCategories job = new UserCategories(DATE, 3);
        job.loadProperties("src/test/pig/user_categories.properties");

        ExecJob results = job.call();
        check(results);

        for (Record record : expectedMultiDays) check(record);
    }

    private void check(ExecJob result) {
        assertEquals(result.getStatus(), ExecJob.JOB_STATUS.COMPLETED);
    }

    private void check(Record record) throws Exception {
        String key = record.getUuid();

        String json = (String) client.get(key);
        assertNotNull(json);

        Record retrieved = Record.fromJson(json);
        assertEquals(retrieved, record);

        assertTrue(client.delete(key).get());
    }

}

