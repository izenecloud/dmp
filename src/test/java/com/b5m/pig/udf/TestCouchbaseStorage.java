package com.b5m.pig.udf;

import com.b5m.utils.Tuples;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.ResourceSchema;

public class TestCouchbaseStorage {

    private CouchbaseStorage func = new CouchbaseStorage();

    @DataProvider
    public Object[][] schemas() {
        return new Object[][] {
            { "chararray", false },                 // not size 2
            { "chararray, chararray, int", false },

            { "int, (int)", false },                // 1st must be text

            { "chararray, chararray", false },      // 2nd must be tuple
            { "chararray, int", false },
            { "chararray, map[]", false },
            { "chararray, {(chararray, int)}", false },

            { "chararray, (chararray)", true },
            { "chararray, (int)", true },
            { "chararray, (map[])", true },
            { "chararray, (chararray, int)", true },
        };
    }

    @Test(dataProvider="schemas")
    public void checkSchema(String schema, boolean valid) {
        try {
            func.checkSchema(Tuples.resourceSchema(schema));
            //func.prepareToWrite(null);
            assertTrue(valid);
        } catch (Exception e) {
            assertFalse(valid, e.getMessage());
        }
    }

}

