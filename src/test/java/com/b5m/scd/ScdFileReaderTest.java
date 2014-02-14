package com.b5m.scd;

import static org.testng.Assert.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups={"scd"})
public class ScdFileReaderTest {

    private InputStream is;
    private ScdFileReader reader;

    @AfterMethod
    public void close() throws Exception {
        reader.close();
        is.close();
    }

    @Test
    public void test1() throws Exception {
        File scd = new File("src/test/data/short.scd");
        is = new FileInputStream(scd);
        reader = new ScdFileReader(is);

        List<String> actual = new ArrayList<String>();
        while (reader.hasNext()) {
            Document d = reader.next();
            Entry id = d.entries.get(0);
            assertEquals(id.untag(), "DOCID");
            actual.add(id.toString());
        }

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
        is = new FileInputStream(scd);
        reader = new ScdFileReader(is);

        int count = 0;
        while (reader.hasNext()) {
            reader.next();
            count++;
        }

        assertEquals(count, 89);
    }

}
