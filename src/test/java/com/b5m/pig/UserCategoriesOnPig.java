package com.b5m.pig;

import com.b5m.utils.DatesGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private final static Log log = LogFactory.getLog(UserCategoriesOnPig.class);

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
    private static void getExpected() throws Exception {
        getRecords("src/test/data/user_categories_one.output", expectedOneDay);
        getRecords("src/test/data/user_categories_multi.output", expectedMultiDays);
    }

    private static void getRecords(String file, List<Record> list) throws Exception {
        List<String> lines = FileUtils.readLines(new File(file));
        for (String line : lines) {
            log.debug(line);
            list.add(Record.fromPig(line));
        }
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
        String key = String.format("%s::%s", record.getUuid(), record.getDate());
        log.debug("Checking for " + key + " in Couchbase");

        String json = (String) client.get(key);
        assertNotNull(json);
        log.debug("Got document: " + json);

        Record retrieved = Record.fromJson(json);
        assertEquals(retrieved, record);

        assertTrue(client.delete(key).get());
    }

}

