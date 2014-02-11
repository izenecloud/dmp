package com.b5m.scd;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.List;

@Test(groups={"scd"})
public class ScdFileFilterTest {

    @Test
    public void filter() {
        List<File> files = ScdFileFilter.scdFilesIn(new File("src/test/data"));
        assertEquals(files, Arrays.asList(new File("src/test/data/test.scd")));
    }

}

