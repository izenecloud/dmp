package com.b5m.scd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.lang.StringUtils;

/**
 * Parse an SCD file and output all Title-Category pairs.
 */
public class DataExtractor implements Callable<File> {

    public final static char SEPARATOR = '\t';

    private static final Log log = LogFactory.getLog(DataExtractor.class);

    private final File scdFile;
    private final File outputFile;
    private final boolean onlyTopCategory;

    private String title;
    private String category;
    private boolean firstDocument = true;

    public DataExtractor(File scdFile, File outputDir) {
        this(scdFile, outputDir, true);
    }

    public DataExtractor(File scdFile, File outputDir, boolean onlyTopCategory) {
        this.scdFile = scdFile;
        this.outputFile = new File(outputDir, scdFile.getName() + ".out");
        this.onlyTopCategory = onlyTopCategory;

        if (log.isDebugEnabled())
            log.debug(String.format("in: [%s] out: [%s] onlyTop: [%s]",
                                    scdFile, outputDir, onlyTopCategory));
    }

    @Override
    public File call() throws IOException {
        if (log.isDebugEnabled())
            log.debug("creating empty output file: " + outputFile);
        outputFile.createNewFile();

        BufferedReader reader = new BufferedReader(new FileReader(scdFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        // parse
        long counter = 0L;
        while (nextDocument(reader)) {
            if (StringUtils.isBlank(title) || StringUtils.isBlank(category)) {
                if (log.isDebugEnabled())
                    log.debug("skipping current document due to empty title/category");
                continue;
            }

            writer.append(title).append(SEPARATOR).append(category).append('\n');
            counter++;
        }

        reader.close();
        writer.close();

        if (log.isDebugEnabled())
            log.debug(String.format("found %d items", counter));
        return outputFile;
    }

    private boolean nextDocument(BufferedReader reader) throws IOException {
        String line = null;
        while (null != (line = reader.readLine())) {
            if (line.startsWith("<DOCID>")) {
                if (firstDocument) {
                    firstDocument = false;
                    continue;
                }
                return true;
            } else if (line.startsWith("<Title>")) {
                title = line.substring(7);
            } else if (line.startsWith("<Category>")) {
                category = substring(line.substring(10));
            }
        }

        return false;
    }

    private String substring(String category) {
        int i = onlyTopCategory ? category.indexOf('>') : category.lastIndexOf('>');
        return (i == -1) ? category : category.substring(0, i);
    }

}
