package com.b5m.pig;

import com.b5m.utils.Dates;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.util.Arrays;

/**
 * Main class for the UserCategories job.
 *
 * @author Paolo D'Apice
 */
public final class UserCategoriesMain {

    private final static Log log = LogFactory.getLog(UserCategoriesMain.class);

    /* Define command line options. */
    private static Options buildOptions() {
        Options options = new Options();

        options.addOption("d", "date", true, "start date in format YYYY-MM-DD");
        options.addOption("c", "count", true, "dates count");
        options.addOption("f", "file", true, "properties file");

        return options;
    }

    /* Container for command line arguments. */
    static class Parameters {
        String date;
        String file;
        int count = 1;
    }

    /* Parse command line. */
    private static boolean parseOptions(Options options, String[] args, Parameters params) {
        if (args.length < 2) return false;
        log.debug("args: " + Arrays.toString(args));

        CommandLineParser parser = new BasicParser();
        try {
            CommandLine line = parser.parse(options, args);

            if (log.isDebugEnabled()) {
                for (Option o : line.getOptions()) {
                    log.debug("option: " + o.toString() + " = " + o.getValue());
                }
                log.debug("nonrec: " + Arrays.toString(line.getArgs()));
            }

            if (line.hasOption("date")) {
                String date = line.getOptionValue("date");
                try {
                    Dates.fromString(date, "yyyy-MM-dd");
                    params.date = date;
                } catch (IllegalArgumentException ex) {
                    System.out.println("Invalid date: " + date);
                    return false;
                }
            } else {
                System.out.println("Required argument 'date'");
                return false;
            }

            if (line.hasOption("count")) {
                String val = line.getOptionValue("count");
                try {
                    params.count = Integer.parseInt(val);
                } catch (NumberFormatException ex) {
                    System.out.println("Not a number: " + val);
                    return false;
                }
            }

            if (line.hasOption("file")) {
                String file = line.getOptionValue("file");
                if (! new File(file).exists()) {
                    System.out.println("Cannot read file: " + file);
                    return false;
                }
                params.file = file;
            } else {
                System.out.println("Required argument 'file'");
                return false;
            }
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            return false;
        }

        return true;
    }

    /* Show usage. */
    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(UserCategoriesMain.class.getName(), options);
    }

    /* Start the job. */
    public static void main(String[] args) throws Exception {
        Options options = buildOptions();
        Parameters params = new Parameters();
        boolean ok = parseOptions(options, args, params);
        if (!ok) {
            usage(options);
            System.exit(1);
        }

        UserCategories job = new UserCategories(params.date, params.count);
        job.loadProperties(params.file);
        job.call();
    }

}

