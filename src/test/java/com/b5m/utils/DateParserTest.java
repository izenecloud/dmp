package com.b5m.utils;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.joda.time.DateTime;

@Test(groups={"utils"})
public class DateParserTest {

    @DataProvider
    public Object[][] dates() {
        return new Object[][] {
            { "yyyy-MM-dd", "2014-02-21", DatesGeneratorTest.newDateTime(2014, 2, 21) },
            { "dd/MM/yy", "21/2/14", DatesGeneratorTest.newDateTime(2014, 2, 21) },
        };
    }

    @Test(dataProvider="dates")
    public void parse(String format, String string, DateTime expected) {
        DateParser parser = new DateParser(format);
        assertEquals(parser.fromString(string), expected);
    }

    @Test(dataProvider="dates")
    public void format(String format, String expected, DateTime date) {
        DateParser parser = new DateParser(format);
        assertEquals(parser.toString(date), expected);
    }
}
