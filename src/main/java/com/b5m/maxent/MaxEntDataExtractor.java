package com.b5m.maxent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get data from SCD file.
 */
// TODO use Callable
class MaxEntDataExtractor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MaxEntDataExtractor.class);

    private final File scdFile;
    private final File output;

    private String title;
    private String category;
    private boolean firstDocument = true;

    MaxEntDataExtractor(File scdFile, File output) {
        this.scdFile = scdFile;
        this.output = output;
    }

    @Override
    public void run() {
        try {
            File trainFile = new File(output, scdFile.getName());
            trainFile.createNewFile();
            log.debug("train file: " + trainFile);

            BufferedReader reader = new BufferedReader(new FileReader(scdFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(trainFile));

            // parse
            while (nextDocument(reader)) {
                if (title.isEmpty() || category.isEmpty()) {
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
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
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
