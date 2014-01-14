package com.b5m.utils;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DatesGeneratorTest {

    private final DatesGenerator gen = new DatesGenerator();

    @Test
    public void today() {
        String expected = new SimpleDateFormat(DatesGenerator.DEFAULT_FORMAT)
                            .format(new Date());

        String date = gen.today();
        assertEquals(date, expected);
    }

    @DataProvider
    public Object[][] inputs() {
        return new Object[][] {
            { "20140101", 1, Arrays.asList("20140101") },
            { "20140110", 3, Arrays.asList("20140110", "20140109", "20140108") },
            { "20140101", 2, Arrays.asList("20140101", "20131231") },
            { "20120303", 5, Arrays.asList("20120303","20120302","20120301","20120229", "20120228") },
        };
    }

    @Test(dataProvider="inputs")
    public void dates(String date, int count, List<String> expected) {
        List<String> dates = gen.getDates(date, count);
        assertEquals(dates, expected);
    }

}

