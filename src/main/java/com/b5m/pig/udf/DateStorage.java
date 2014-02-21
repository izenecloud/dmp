package com.b5m.pig.udf;

import com.b5m.utils.DateParser;
import com.b5m.utils.DatesGenerator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;

import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Store function writing into a directory tree built from a date.
 * For example, given the date "2012-12-20" and storage directory "output",
 * then data is stored into "output/2012/12/20".
 * 
 * @author Paolo D'Apice
 */
public class DateStorage extends PigStorage {

    private final DateTime date;
    private final int count;

    public DateStorage(String date) {
        this(date, "1");
    }

    public DateStorage(String date, String count) {
        // TODO use Options to specify format
        this.date = new DateParser().fromString(date);
        this.count = Integer.parseInt(count);
    }
    
    @Override
    public void setLocation(String location, Job job)
    throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        String inputs = getInputs(location, fs); 
        if (mLog.isDebugEnabled())
            mLog.debug("input locations: " + inputs);
        super.setLocation(inputs, job);
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    throws IOException {
        String dateLocation = getDateLocation(location, date);
        if (mLog.isDebugEnabled())
            mLog.debug("date location: " + dateLocation);
        return super.relToAbsPathForStoreLocation(dateLocation, curDir);
    }
    
    private String getInputs(String location, FileSystem fs) throws IOException {
        // generate dates
        DatesGenerator dategen = new DatesGenerator();

        // get input filenames (if file exists)
        List<String> inputs = new ArrayList<String>();
        for (DateTime d : dategen.getDates(date, count)) {
            String loc = getDateLocation(location, d);
            if (fs.exists(new Path(loc))) {
                if (mLog.isInfoEnabled()) mLog.info("adding input: " + loc);
                inputs.add(loc);
            } else {
                mLog.warn("skipping non-existing input: " + loc);
            }
        }
        
        // convert to a comma-separated list
        StringBuilder sb = new StringBuilder();
        for (String s : inputs) sb.append(s).append(",");
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    private String getDateLocation(String location, DateTime date) {
        return String.format("%s/%d/%02d/%02d", location,
                date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
    }
    
}
