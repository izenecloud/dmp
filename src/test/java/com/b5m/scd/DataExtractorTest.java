package com.b5m.scd;

import com.b5m.utils.Files;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;

import java.io.File;

@Test(groups={"scd"})
public class DataExtractorTest {

    private File input;
    private File outputDir;
    private File expectedTop;
    private File expectedFull;

    @BeforeTest
    public void setup() throws Exception {
        input = new File("src/test/data/test.scd");
        outputDir = Files.tempDir("DataExtractor");
        expectedTop = new File("src/test/data/title-category-top.txt");
        expectedFull = new File("src/test/data/title-category-full.txt");
    }

    @AfterTest
    public void cleanup() throws Exception {
        FileUtils.deleteDirectory(outputDir);
    }

    @Test
    public void topCategory() throws Exception {
        DataExtractor de = new DataExtractor(input, outputDir);

        File out = de.call();
        checkFile(out, expectedTop);
    }

    @Test
    public void fullCategory() throws Exception {
        DataExtractor de = new DataExtractor(input, outputDir);
        de.onlyTop(false);

        File out = de.call();
        checkFile(out, expectedFull);
    }

    private void checkFile(File actual, File expected)
    throws Exception{
        assertTrue(actual.isFile());
        assertEquals(actual.getName(), input.getName() + ".out");
        assertEquals(FileUtils.readLines(actual),
                     FileUtils.readLines(expected));
    }

}

