package com.b5m.maxent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

/**
 * Parse an SCD file and output all Title-Category pairs.
 */
class MaxEntDataExtractor implements Callable<File> {

    private static final Logger log = LoggerFactory.getLogger(MaxEntDataExtractor.class);

    private final File scdFile;
    private final File outputDir;
    private final File outputFile;

    private boolean onlyTop = true;

    private String title;
    private String category;
    private boolean firstDocument = true;

    MaxEntDataExtractor(File scdFile, File outputDir) {
        this.scdFile = scdFile;
        this.outputDir = outputDir;
        outputFile = new File(outputDir, scdFile.getName() + ".out");

        log.debug("scdFile   : " + scdFile);
        log.debug("outputFile: " + outputFile);
    }

    public void onlyTop(boolean val) {
        onlyTop = val;
    }

    @Override
    public File call() throws IOException {
        log.debug("creating empty output file: " + outputFile);
        outputFile.createNewFile();

        BufferedReader reader = new BufferedReader(new FileReader(scdFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        // parse
        while (nextDocument(reader)) {
            if (StringUtils.isBlank(title) || StringUtils.isBlank(category)) {
                log.debug("skipping current document due to empty title/category");
                continue;
            }

            // XXX is it better us '\t' instead of ' ' ?
            writer.append(title).append(' ').append(category).append('\n');
        }

        reader.close();
        writer.close();

        log.debug("done");
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
                category = substring(line.substring(10), onlyTop);
            }
        }

        return false;
    }

    private String substring(String category, boolean first) {
        int i = first ? category.indexOf('>') : category.lastIndexOf('>');
        return (i == -1) ? category : category.substring(0, i);
    }

}
