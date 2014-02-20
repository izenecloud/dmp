package com.b5m.maxent;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import opennlp.model.Event;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

@Test(groups={"maxent"})
public class FileEventStreamTest {

    private Reader reader;
    private FileEventStream eventStream;
    
    @BeforeTest
    public void setup() throws Exception {
        reader = new FileReader(new File("src/test/data/title-category-top.txt"));
    }

    @AfterTest
    public void teardown() throws Exception {
        reader.close();
    }

    @Test
    public void test() throws Exception {
        eventStream = new FileEventStream(reader);

        int numEvents = 0;
        while (eventStream.hasNext()) {
            Event e = eventStream.next();
            numEvents++;
        }
        assertEquals(numEvents, 84);
    }

}

