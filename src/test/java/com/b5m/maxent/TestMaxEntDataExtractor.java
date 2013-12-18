package com.b5m.maxent;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class TestMaxEntDataExtractor {

    private final File scdFile;
    private final File outputDir;

    public TestMaxEntDataExtractor() throws IOException {
        URL url =  getClass().getResource("/B-00-201312091124-16026-U-C.SCD");
        scdFile = new File(url.getFile());

        outputDir = new File(tmpdir(), "test");
        outputDir.mkdir();
        outputDir.deleteOnExit();
    }

    @Test
    public void test() {
        MaxEntDataExtractor de = new MaxEntDataExtractor(scdFile, outputDir);
        de.run();

        File out = de.getOutputFile();
        assertTrue(out.exists() && out.isFile());

        checkOutput(out);
    }

    private void checkOutput(File file) {
        try {
            System.out.println(FileUtils.readFileToString(file));
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    private File tmpdir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }
}

