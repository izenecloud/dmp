package com.b5m.utils;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Test(groups={"utils"})
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
            { "2014-01-01", 1, Arrays.asList("2014-01-01") },
            { "2014-01-10", 3, Arrays.asList("2014-01-10", "2014-01-09", "2014-01-08") },
            { "2014-01-01", 2, Arrays.asList("2014-01-01", "2013-12-31") },
            { "2012-03-03", 5, Arrays.asList("2012-03-03","2012-03-02","2012-03-01","2012-02-29", "2012-02-28") },
        };
    }

    @Test(dataProvider="inputs")
    public void dates(String date, int count, List<String> expected) {
        List<String> dates = gen.getDates(date, count);
        assertEquals(dates, expected);
    }

}

