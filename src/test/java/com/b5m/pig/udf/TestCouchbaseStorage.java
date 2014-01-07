package com.b5m.pig.udf;

import com.b5m.utils.Tuples;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.ResourceSchema;

import java.io.IOException;

public class TestCouchbaseStorage {

    private CouchbaseStorage func = new CouchbaseStorage();

    @DataProvider
    public Object[][] schemas() throws Exception {
        return new Object[][] {
            { Tuples.resourceSchema("chararray"), false },
            { Tuples.resourceSchema("chararray, chararray"), true },
            { Tuples.resourceSchema("chararray, int"), true },
            { Tuples.resourceSchema("int, int"), false },
            { Tuples.resourceSchema("chararray, chararray, int"), false },
            { Tuples.resourceSchema("chararray, (chararray, int)"), true },
            { Tuples.resourceSchema("chararray, {(chararray, int)}"), true },
            { Tuples.resourceSchema("chararray, map[]"), true },
        };
    }

    @Test(dataProvider="schemas")
    public void checkSchema(ResourceSchema schema, boolean valid) {
        try {
            func.checkSchema(schema);
            assertTrue(valid);
        } catch (IOException e) {
            assertFalse(valid, e.getMessage());
        }
    }

}

