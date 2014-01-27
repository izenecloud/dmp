package com.b5m.pig;

import com.b5m.utils.DatesGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;

import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Compute user categories using daily log analysis over a period.
 *
 * @author Paolo D'Apice
 */
public class UserCategories implements Callable<ExecJob> {

    private final static Log log = LogFactory.getLog(UserCategories.class);

    private String date;
    private final int count;

    private Properties properties = new Properties();
    private PigServer pig = null;

    /**
     * Instantiate for analysis on <code>count</code> days
     * starting from <code>date</code>.
     */
    public UserCategories(String date, int count) {
        this.date = date;
        this.count = count;
    }

    /**
     * Load properties from file.
     */
    public void loadProperties(String file)
    throws FileNotFoundException, IOException {
        properties.load(new FileInputStream(file));
        if (log.isDebugEnabled()) log.debug("loaded properties: " + properties);
    }

    private List<String> getInputs(String basedir, List<String> dates) throws IOException {
        List<String> inputs = new ArrayList<String>(dates.size());
        for (String d : dates) {
            String input = String.format("%s/%s", basedir, d);
            if (pig.existsFile(input)) {
                if (log.isDebugEnabled()) log.debug("adding input: " + input);
                inputs.add(input);
            }
        }
        return inputs;
    }

    private String listToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (String s : list) sb.append(s).append(",");
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private FuncSpec function(String name, String ... args) {
        FuncSpec fs = new FuncSpec(name, args);
        if (log.isDebugEnabled()) log.debug("function: " + fs);
        return fs;
    }

    @Override
    public ExecJob call() throws ExecException, IOException {
        String mode = properties.getProperty("mode", "mapreduce");
        pig = new PigServer(mode);

        // generate dates
        DatesGenerator dategen = new DatesGenerator();
        List<String> dates = dategen.getDates(date, count);
        if (log.isDebugEnabled()) log.debug("dates: " + dates);

        // get input filenames (if file exists)
        String basedir = properties.getProperty("basedir");
        List<String> inputs = getInputs(basedir, dates);
        if (log.isDebugEnabled()) log.debug("inputs: " + inputs);

        // registers UDFs
        pig.registerFunction("Normalize", function("com.b5m.pig.udf.NormalizeMap"));
        pig.registerFunction("Merge", function("com.b5m.pig.udf.MergeMaps"));

        // load input files
        pig.registerQuery(String.format("daily = LOAD '%s' USING JsonLoader();", listToString(inputs)));

        StringBuilder stmt = new StringBuilder();

        // compute period analytics
        pig.registerQuery("grouped = GROUP daily BY uuid;");
        stmt.append("analytics = FOREACH grouped {")
            .append("  merged = Merge(daily.categories);")
            .append("  normalized = Normalize(merged);")
            .append("  GENERATE")
            .append("    group AS uuid,")
            .append("    normalized AS categories;")
            .append("}");
        pig.registerQuery(stmt.toString());
        //dump("analytics");

        stmt = new StringBuilder();

        // prepare key-value documents for Couchbase
        stmt.append("documents = FOREACH analytics GENERATE")
            .append("  CONCAT(uuid, '::").append(date).append("') AS key,")
            .append("  TOTUPLE(uuid,'").append(date).append("',").append(count).append(",categories)")
            // explicit schema so that fields have name and can be correctly serialized to Json
            .append("    AS value:(uuid:chararray, date:chararray, period:int, categories:[double]);");
        pig.registerQuery(stmt.toString());
        //dump("documents");

        // XXX there's a bug in PigServer so it won't use previously registered
        //     functions in method store; as workaround directly use a string
        FuncSpec storefunc = function("com.b5m.pig.udf.CouchbaseStorage",
                          properties.getProperty("hosts"),
                          properties.getProperty("bucket"),
                          properties.getProperty("password"),
                          properties.getProperty("batchSize")
                    );

        ExecJob job = pig.store("documents", "output", storefunc.toString());
        if (log.isInfoEnabled()) log.info("job status: " + job.getStatus());

        return job;
    }

    /* only for tests */
    private void dump(String alias) throws IOException {
        pig.dumpSchema(alias);
        Iterator<Tuple> data = pig.openIterator(alias);
        while (data.hasNext()) {
            System.out.println(data.next().toString());
        }
    }
}

// vim: set nospell:
