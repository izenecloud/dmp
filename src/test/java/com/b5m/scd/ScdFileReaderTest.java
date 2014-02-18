package com.b5m.scd;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups={"scd"})
public class ScdFileReaderTest {

    private ScdFileReader reader;

    @Test
    public void test1() throws Exception {
        File scd = new File("src/test/data/short.scd");
        reader = new ScdFileReader(new FileInputStream(scd));

        List<String> actual = new ArrayList<String>();
        while (reader.nextDocument()) {
            Document d = reader.getCurrentDocument();
            Entry id = d.entries.get(0);
            assertEquals(id.getTagName(), "DOCID");
            actual.add(id.toString());
        }
        reader.close();

        List<String> expected = Arrays.asList(
                "<DOCID>e67af43226589c9399a76d85e04fac62",
                "<DOCID>e67af85fdcee6555af5ae4b37550ebec",
                "<DOCID>3abc788c06e96fb8c0a333698c7fd9c5"
            );
        assertEquals(actual, expected);
    }

    @Test
    public void test2() throws Exception {
        File scd = new File("src/test/data/test.scd");
        reader = new ScdFileReader(new FileInputStream(scd));

        int count = readDocuments();
        reader.close();

        assertEquals(count, 89);
    }

    @Test
    public void test3() throws Exception {
        Map<String, Integer> expected = new HashMap<String, Integer>();
        expected.put("B-00-201312091124-16026-U-C.SCD", 8);
        expected.put("B-00-201312091124-16027-U-C.SCD", 12);
        expected.put("B-00-201312091124-16028-U-C.SCD", 5);
        expected.put("B-00-201312091124-16029-U-C.SCD", 7);
        expected.put("B-00-201312091124-16030-U-C.SCD", 8);
        expected.put("B-00-201312091124-16031-U-C.SCD", 6);
        expected.put("B-00-201312091124-16032-U-C.SCD", 7);
        expected.put("B-00-201312091124-16033-U-C.SCD", 5);
        expected.put("B-00-201312091124-16034-U-C.SCD", 8);
        expected.put("B-00-201312091124-16035-U-C.SCD", 7 );

        File dir = new File("src/test/data/maxent");
        File[] files = dir.listFiles(new ScdFileFilter());
        for (File f : files) {
            reader = new ScdFileReader(new FileInputStream(f));
            
            int count = readDocuments();
            assertEquals(expected.get(f.getName()).intValue(), count);
            
            reader.close();
        }
    }

    private int readDocuments() throws Exception {
        int count = 0;
        while (reader.nextDocument()) {
            reader.getCurrentDocument();
            count++;
        }
        return count;
    }

}
