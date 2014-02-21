package com.b5m.utils;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

@Test(groups={"utils"})
public class DatesGeneratorTest {

    private final DatesGenerator gen = new DatesGenerator();

    static DateTime newDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, 0, 0);
    }

    @DataProvider
    public Object[][] inputs() {
        return new Object[][] {
            { 
                newDateTime(2014,1,1), 1,
                Arrays.asList(newDateTime(2014,1,1))
            },
            {
                newDateTime(2014,1,10), 3,
                Arrays.asList(
                        newDateTime(2014,1,10),
                        newDateTime(2014,1,9),
                        newDateTime(2014,1,8))
            },
            { 
                newDateTime(2014,1,1), 2,
                Arrays.asList(
                        newDateTime(2014,1,1),
                        newDateTime(2013,12,31))
            },
            {
                newDateTime(2012,3,3), 5,
                Arrays.asList(
                        newDateTime(2012,3,3),
                        newDateTime(2012,3,2),
                        newDateTime(2012,3,1),
                        newDateTime(2012,2,29),
                        newDateTime(2012,2,28))
            },
        };
    }

    @Test(dataProvider="inputs")
    public void dates(DateTime date, int count, List<DateTime> expected) {
        List<DateTime> dates = gen.getDates(date, count);
        assertEquals(dates, expected);
    }

}

