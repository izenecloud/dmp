package com.b5m.maxent;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import opennlp.model.EventStream;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Test(groups={"maxent"})
public class FilesEventStreamTest {

    private final static String SEPARATOR = "\t";

    private List<File> files;

    @BeforeTest
    public void setup() {
        files = Arrays.asList(
                new File("src/test/data/title-category-top.txt"),
                new File("src/test/data/title-category-top.txt"),
                new File("src/test/data/title-category-top.txt"));
    }

    @Test
    public void none() throws Exception {
        EventStream es = new FilesEventStream(Collections.<File>emptyList(), SEPARATOR);
        assertFalse(es.hasNext());
    }

    @Test
    public void top() throws Exception {
        EventStream es = new FilesEventStream(files, SEPARATOR);
        int numEvents = 0;
        while (es.hasNext()) {
            es.next();
            numEvents++;
        }

        assertEquals(numEvents, 3*84);
    }

}

