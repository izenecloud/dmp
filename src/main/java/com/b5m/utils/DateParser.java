package com.b5m.utils;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Parse and convert dates.
 *
 * @author Paolo D'Apice
 */
public final class DateParser {

    public final static String DEFAULT_FORMAT = "yyyy-MM-dd";
            
    private final DateTimeFormatter formatter;

    public DateParser() {
        this(DEFAULT_FORMAT);
    }

    public DateParser(String format) {
        formatter = DateTimeFormat.forPattern(format);
    }

    /**
     * Convert a date to string.
     */
    public String toString(DateTime date) {
        return new LocalDate(date).toString(formatter);
    }

    /**
     * Parse a date from string. 
     */
    public DateTime fromString(String string) {
        return formatter.parseDateTime(string);
    }

    /**
     * Convert a date to string.
     */
    public static String toString(DateTime date, String format) {
        return new DateParser(format).toString(date);
    }

    /**
     * Parse a date from string.
     */
    public static DateTime fromString(String string, String format) {
        return new DateParser(format).fromString(string);
    }

}
