package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;

public class TestCouchbaseStorage {

    private CouchbaseStorage func = new CouchbaseStorage();

    @DataProvider
    public Object[][] schemas() throws Exception {
        return new Object[][] {
            { newResourceSchema("chararray"), false },
            { newResourceSchema("chararray, chararray"), true },
            { newResourceSchema("chararray, int"), true },
            { newResourceSchema("int, int"), false },
            { newResourceSchema("chararray, chararray, int"), false },
            { newResourceSchema("chararray, (chararray, int)"), true },
            { newResourceSchema("chararray, {(chararray, int)}"), true },
            { newResourceSchema("chararray, map[]"), true },
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

    private ResourceSchema newResourceSchema(String string) throws Exception {
        return new ResourceSchema(Utils.getSchemaFromString(string));
    }
}

