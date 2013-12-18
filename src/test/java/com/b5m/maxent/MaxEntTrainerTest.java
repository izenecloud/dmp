package com.b5m.maxent;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import com.b5m.utils.Files;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class MaxEntTrainerTest {

    private final File scdDir;
    private final File outDir;

    public MaxEntTrainerTest() throws IOException {
        URL url =  getClass().getResource("/B-00-201312091124-16026-U-C.SCD");
        scdDir = new File(url.getFile()).getParentFile();
        outDir = Files.tempDir("MaxEntTrainerTest", false);
    }

    @Test
    public void test() throws Exception {
        String[] args = { scdDir.toString(), outDir.toString() };
        MaxEntTrainer.main(args);
    }

}
