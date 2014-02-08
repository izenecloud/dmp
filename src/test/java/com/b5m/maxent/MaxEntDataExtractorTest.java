package com.b5m.maxent;

import com.b5m.utils.Files;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

@Test(groups={"maxent"})
public class MaxEntDataExtractorTest {

    private final File scdFile;
    private final File outputDir;

    public MaxEntDataExtractorTest() throws IOException {
        scdFile = Files.getResource("/B-00-201312091124-16026-U-C.SCD");
        outputDir = Files.tempDir("MaxEntDataExtractor", true);
    }

    @Test
    public void test() throws IOException {
        MaxEntDataExtractor de = new MaxEntDataExtractor(scdFile, outputDir);

        File out = de.call();
        assertTrue(out.exists() && out.isFile());
        assertEquals(out.getName(), "B-00-201312091124-16026-U-C.SCD.out");

        // TODO
        //checkOutput(out);
    }

    private void checkOutput(File file) throws IOException {
        System.out.println(FileUtils.readFileToString(file));
    }
}

