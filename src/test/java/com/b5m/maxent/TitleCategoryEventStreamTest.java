package com.b5m.maxent;

import com.b5m.scd.DataExtractor;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import opennlp.model.EventStream;
import opennlp.model.Event;

import java.io.File;
import java.io.FileReader;

@Test(groups={"maxent"})
public class TitleCategoryEventStreamTest {

    private FileReader fr;

    @BeforeTest
    public void setup() throws Exception {
        fr = new FileReader(new File("src/test/data/title-category-top.txt"));
    }

    @AfterTest
    public void teardown() throws Exception {
        fr.close();
    }

    @Test
    public void top() throws Exception {
        EventStream es = new TitleCategoryEventStream(fr, Character.toString(DataExtractor.SEPARATOR));

        int numEvents = 0;
        while (es.hasNext()) {
            Event e = es.next();
            System.out.println(e);
            numEvents++;
        }

        assertEquals(numEvents, 84);
    }

}

