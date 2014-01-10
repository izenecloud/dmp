package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.data.Tuple;

import java.util.Arrays;
import java.util.List;

public class DatesGeneratorTest {

    private DatesGenerator gen;

    @Test
    public void today() throws Exception {
        String date = "20140110";

        gen = new DatesGenerator(date);
        gen.prepareToRead(null, null);

        Tuple tuple = gen.getNext();
        assertEquals(tuple.size(), 1);
        assertEquals((String) tuple.get(0), date);

        assertNull(gen.getNext());
    }

    @DataProvider
    public Object[][] inputs() {
        return new Object[][] {
            { "20140110", "3", Arrays.asList("20140110", "20140109", "20140108") },
            { "20140101", "2", Arrays.asList("20140101", "20131231") },
            { "20120303", "5", Arrays.asList("20120303","20120302","20120301","20120229", "20120228") },
        };
    }

    @Test(dataProvider="inputs")
    public void dates(String date, String count, List<String> expected)
    throws Exception {
        gen = new DatesGenerator(date, count);
        gen.prepareToRead(null, null);

        int n = 0;
        Tuple tuple = null;

        while ((tuple = gen.getNext()) != null) {
            assertEquals(tuple.size(), 1);
            assertEquals((String) tuple.get(0), expected.get(n++));
        }

        assertEquals(Integer.parseInt(count), n);
    }

}

