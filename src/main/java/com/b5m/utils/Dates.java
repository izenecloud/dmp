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

    /** Convert a date to string. */
    public static String toString(Date date, String format) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(format);
        LocalDate ld = new LocalDate(date);
        return ld.toString(dtf);
    }

    /** Parse a date from string. */
    public static Date fromString(String string) {
        return formatter.parseDateTime(string).toDate();
    }

    /** Parse a date from string. */
    public static Date fromString(String string, String format) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(format);
        return dtf.parseDateTime(string).toDate();
    }

    private Dates() {} // prevent instantiation
}

