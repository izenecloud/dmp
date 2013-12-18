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

/**
 * Get data from SCD file.
 */
// TODO use Callable
class MaxEntDataExtractor implements Callable<File> {

    private static final Logger log = LoggerFactory.getLogger(MaxEntDataExtractor.class);

    private final File scdFile;
    private final File outputDir;
    private final File outputFile;

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

    @Override
    public File call() throws IOException {
        outputFile.createNewFile();
        log.debug("created empty output file: " + outputFile);

        BufferedReader reader = new BufferedReader(new FileReader(scdFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        // parse
        while (nextDocument(reader)) {
            if (title.isEmpty() || category.isEmpty()) {
                log.debug("skipping current document due to empty title/category");
                continue;
            }
            int topLevelIndex = category.indexOf('>');
            if (-1 == topLevelIndex)
                writer.write(title + " " + category + "\n");
            else
                writer.write(title + " "
                        + category.substring(0, topLevelIndex) + "\n");
        }

        reader.close();
        writer.close();

        log.debug("done");
        return outputFile;
    }

    private boolean nextDocument(BufferedReader reader) throws IOException {
        //title = new String();
        //category = new String();
        boolean hasNext = false;
        String line = null;
        while (null != (line = reader.readLine())) {
            if (line.startsWith("<DOCID>")) {
                if (firstDocument) {
                    firstDocument = false;
                    continue;
                }
                hasNext = true;
                break;
            } else if (line.startsWith("<Title>")) {
                title = line.substring(7);
            } else if (line.startsWith("<Category>")) {
                category = line.substring(10);
            }
        }

        return hasNext;
    }

}
