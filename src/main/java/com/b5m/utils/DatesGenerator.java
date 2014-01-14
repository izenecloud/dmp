package com.b5m.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator of dates in string format.
 *
 * @author Paolo D'Apice
 */
public final class DatesGenerator {

    private final static Log log = LogFactory.getLog(DatesGenerator.class);

    /** Default date format. */
    public final static String DEFAULT_FORMAT = "yyyyMMdd";

    private final DateTimeFormatter format;

    /**
     * Generator with default format.
     */
    public DatesGenerator() {
        this(DEFAULT_FORMAT);
    }

    /**
     * Generator with given format.
     */
    public DatesGenerator(String format) {
        if (log.isDebugEnabled()) log.debug("format=" + format);
        this.format = DateTimeFormat.forPattern(format);
    }

    /**
     * Generates string with today's date.
     */
    public String today() {
        String date = new LocalDate().toString(format);
        if (log.isDebugEnabled()) log.debug("today=" + date);

        return date;
    }

    /**
     * Generates list of <code>count</code> string dates
     * starting from today.
     */
    public List<String> getDates(int count) {
        return getDates(today(), count);
    }

    /**
     * Generates list of <code>count</code> string dates
     * starting from the given date.
     */
    public List<String> getDates(String date, int count) {
        if (log.isDebugEnabled()) log.debug("date=" + date + ", count=" + count);

        List<String> dates = new ArrayList<String>();

        LocalDate last = format.parseDateTime(date).toLocalDate();
        for (int i = 0; i < count; i++) {
            dates.add(last.minusDays(i).toString(format));
        }
        if (log.isDebugEnabled()) log.debug("dates: " + dates);

        return dates;
    }

}

