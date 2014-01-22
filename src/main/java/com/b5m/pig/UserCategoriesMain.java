package com.b5m.pig;

import com.b5m.utils.Dates;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;

import java.io.File;

/**
 * Main class for the UserCategories job.
 *
 * @author Paolo D'Apice
 */
public final class UserCategoriesMain {

    private static String date;
    private static String file;
    private static int count;

    /* Define command line options. */
    private static Options buildOptions() {
        Options options = new Options();

        options.addOption("d", "date", true, "start date in format YYYY-MM-DD");
        options.addOption("c", "count", true, "dates count");
        options.addOption("f", "file", true, "properties file");

        return options;
    }

    /* Parse command line. */
    private static boolean parseOptions(Options options, String[] args) {
        if (args.length < 2) return false;

        CommandLineParser parser = new BasicParser();
        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("date")) {
                date = line.getOptionValue("date");
                try {
                    Dates.fromString(date, "yyyy-MM-dd");
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
                    count = Integer.parseInt(val);
                } catch (NumberFormatException ex) {
                    System.out.println("Not a number: " + val);
                    return false;
                }
            } else {
                count = 1;
            }

            if (line.hasOption("file")) {
                file = line.getOptionValue("file");
                if (! new File(file).exists()) {
                    System.out.println("Cannot read file: " + file);
                    return false;
                }
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
        boolean ok = parseOptions(options, args);
        if (!ok) {
            usage(options);
            System.exit(1);
        }

        UserCategories job = new UserCategories(date, count);
        job.addPropertiesFile(file);
        job.call();
    }

}

