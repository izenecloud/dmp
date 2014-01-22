package com.b5m.pig;

import com.b5m.utils.DatesGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;

import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

public class UserCategories implements Callable<ExecJob> {

    private final static Log log = LogFactory.getLog(UserCategories.class);

    private String date;
    private final int count;

    private Properties properties = new Properties();
    private PigServer pig = null;

    public UserCategories(String date) {
        this(date, 1);
    }

    public UserCategories(String date, int count) {
        this.date = date;
        this.count = count;
    }

    public void addPropertiesFile(String file)
    throws FileNotFoundException, IOException {
        properties.load(new FileInputStream(file));
        if (log.isDebugEnabled()) log.debug("loaded properties: " + properties);
    }

    @Override
    public ExecJob call() throws ExecException, IOException {
        String mode = properties.getProperty("mode");
        pig = new PigServer(mode);

        DatesGenerator dategen = new DatesGenerator();
        List<String> dates = dategen.getDates(date, count);

        String basedir = properties.getProperty("basedir");
        StringBuilder input = new StringBuilder();
        for (String d : dates) {
            input.append(basedir).append("/").append(d).append(",");
        }
        input.deleteCharAt(input.length() - 1);

        pig.registerQuery("a = LOAD '" + input.toString() + "' USING JsonLoader();");

        pig.registerFunction("Normalize", function("com.b5m.pig.udf.NormalizeMap"));
        pig.registerFunction("Merge", function("com.b5m.pig.udf.MergeMaps"));

        pig.registerQuery("b = GROUP a BY uuid;");
        pig.registerQuery("c = FOREACH b {"
        + "  m = Merge(a.categories);"
        + "  n = Normalize(m);"
        + "  GENERATE CONCAT(group, '::" + date + "') AS uuid, n AS categories; "
        + "}");

        // XXX there's a bug in PigServer so it won't use previously registered
        //     functions in method store; as workaround directly use a string
        FuncSpec storefunc = function("com.b5m.pig.udf.CouchbaseStorage",
                          properties.getProperty("hosts"),
                          properties.getProperty("bucket"),
                          properties.getProperty("password"),
                          properties.getProperty("batchSize")
                    );
        ExecJob job = pig.store("c", "output", storefunc.toString());

        return job;
    }

    private FuncSpec function(String name, String ... args) {
        FuncSpec fs = new FuncSpec(name, args);
        if (log.isDebugEnabled()) log.debug("function: " + fs);

        return fs;
    }

    public static void main(String[] args) throws Exception {
        // TODO instantiate and launch
    }
}

// vim: set nospell:
