package com.b5m.utils;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

/**
 * Utility class for testing on dates.
 * @author Paolo D'Apice
 */
public final class Dates {

    private final static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");

    /** Convert a date to string. */
    public static String toString(Date date) {
        LocalDate ld = new LocalDate(date);
        return ld.toString(formatter);
    }

    /** Parse a date from string. */
    public static Date fromString(String string) {
        return formatter.parseDateTime(string).toDate();
    }

    private Dates() {} // prevent instantiation
}

