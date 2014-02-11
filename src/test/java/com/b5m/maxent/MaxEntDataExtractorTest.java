package com.b5m.maxent;

import com.b5m.utils.Files;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.commons.io.FileUtils;

import java.io.File;

@Test(groups={"maxent"})
public class MaxEntDataExtractorTest {

    private File input;
    private File outputDir;

    @BeforeTest
    public void setup() throws Exception {
        input = new File("src/test/data/test.scd");
        outputDir = Files.tempDir("MaxEntDataExtractor");
    }

    @AfterTest
    public void cleanup() throws Exception {
        FileUtils.deleteDirectory(outputDir);
    }

    @Test
    public void test() throws Exception {
        MaxEntDataExtractor de = new MaxEntDataExtractor(input, outputDir);

        File out = de.call();
        checkFile(input, out);
    }

    }

    private void checkFile(File input, File actual)
    throws Exception{
        assertTrue(actual.isFile());
        assertEquals(actual.getName(), input.getName() + ".out");
    }

}

