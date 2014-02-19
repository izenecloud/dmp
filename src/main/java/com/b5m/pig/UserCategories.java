package com.b5m.pig;

import com.b5m.utils.DatesGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Compute user categories using daily log analysis over a period.
 *
 * @author Paolo D'Apice
 */
public class UserCategories implements Callable<ExecJob> {

    private final static Log log = LogFactory.getLog(UserCategories.class);

    private final String date;
    private final int count;

    private final Properties properties = new Properties();
    private PigServer pig = null;

    /**
     * Instantiate for analysis on <code>count</code> days
     * starting from <code>date</code>.
     */
    public UserCategories(String date, int count) {
        this.date = date;
        this.count = count;
    }

    /*
     * Load properties from file.
     */
    public void loadProperties(String file)
    throws FileNotFoundException, IOException {
        properties.load(new FileInputStream(file));
        if (log.isDebugEnabled()) log.debug("loaded properties: " + properties);
    }

    @Override
    public ExecJob call() throws ExecException, IOException {
        String mode = properties.getProperty("mode", "mapreduce");
        pig = new PigServer(mode);

        // register script
        InputStream is = getClass().getResourceAsStream("/user_categories.pig");
        pig.registerScript(is, getParameters());
        if (log.isDebugEnabled()) log.debug("registered script");
        
        // execute
        pig.setBatchOn();
        List<ExecJob> jobs = pig.executeBatch();
        if (log.isDebugEnabled()) log.debug("jobs executed: " + jobs.size());

        if (jobs.size() != 1)
            log.warn("Multiple jobs executed: " + jobs.size());

        ExecJob job = jobs.get(0);
        if (log.isInfoEnabled()) log.info("job status: " + job.getStatus());

        pig.shutdown();
        if (log.isInfoEnabled()) log.info("finished");

        return job;
    }

    private String getInput() throws IOException {
        // generate dates
        DatesGenerator dategen = new DatesGenerator();
        List<String> dates = dategen.getDates(date, count);
        if (log.isDebugEnabled()) log.debug("dates: " + dates);

        // get input filenames (if file exists)
        String basedir = properties.getProperty("basedir");
        List<String> inputs = new ArrayList<String>(dates.size());
        for (String d : dates) {
            String input = String.format("%s/%s", basedir, d);
            if (pig.existsFile(input)) {
                if (log.isInfoEnabled()) log.info("adding input: " + input);
                inputs.add(input);
            } else {
                if (log.isInfoEnabled()) log.info("skipping non-existing input: " + input);
            }
        }
        if (log.isDebugEnabled()) log.debug("inputs: " + inputs);

        // convert to a comma-separated list
        StringBuilder sb = new StringBuilder();
        for (String s : inputs) sb.append(s).append(",");
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    private Map<String, String> getParameters() throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        map.put("date", date);
        map.put("count", Integer.toString(count));
        map.put("input", getInput());
        for (String p : new String[]{"hosts", "bucket", "password", "batchSize"}) {
            map.put(p, properties.getProperty(p));
        }
        log.debug("map: " + map);

        return map;
    }

}

// vim:nospell:
