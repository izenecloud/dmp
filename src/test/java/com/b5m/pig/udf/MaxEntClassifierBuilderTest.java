package com.b5m.pig.udf;

import com.b5m.utils.Tuples;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.StoreFunc;

@Test(groups={"pig"})
public class MaxEntClassifierBuilderTest {

    private final StoreFunc func = new MaxEntClassifierBuilder();

    @DataProvider
    public Object[][] schemas() {
        return new Object[][] {
            { "chararray", false },                 // not size 2
            { "chararray, chararray, int", false },

            { "int, (int)", false },                // 1st must be text

            { "chararray, chararray", true },
            { "chararray, int", false },            // 2nd must be text
            { "chararray, map[]", false },
            { "chararray, {(chararray, int)}", false },
        };
    }

    @Test(dataProvider="schemas")
    public void checkSchema(String schema, boolean valid) {
        try {
            func.checkSchema(Tuples.resourceSchema(schema));
            assertTrue(valid);
        } catch (Exception e) {
            assertFalse(valid, e.getMessage());
        }
    }

}

