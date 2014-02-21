package com.b5m.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;

/**
 * Generator of dates.
 *
 * @author Paolo D'Apice
 */
public final class DatesGenerator {

    private final static Log log = LogFactory.getLog(DatesGenerator.class);

    /**
     * Generates list of dates.
     * @param date Start date
     * @param count number of dates
     * @return A List of dates.
     */
    public List<DateTime> getDates(DateTime date, int count) {
        if (log.isDebugEnabled())
            log.debug(String.format("date=%s, count=%d",
                    DateParser.toString(date, DateParser.DEFAULT_FORMAT),
                    count));

        List<DateTime> dates = new ArrayList<DateTime>(count);
        for (int i = 0; i < count; i++) {
            dates.add(date.minusDays(i));
        }

        if (log.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (DateTime d : dates) {
                sb.append(DateParser.toString(d, DateParser.DEFAULT_FORMAT))
                  .append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            log.debug("dates: " + sb.toString());
        }

        return dates;
    }

}
