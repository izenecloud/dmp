package com.b5m.pig.udf;

import com.b5m.utils.Tuples;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CategoryTextTest {
    
    private EvalFunc<String> top, full;

    @BeforeTest
    public void setup() {
        top = new CategoryText();
        full = new CategoryText("full");
    }

    @DataProvider
    public Object[][] data() {
        return new Object[][] {
            // input, full, top
            { "鞋包配饰>女鞋>休闲鞋>", "鞋包配饰>女鞋>休闲鞋", "鞋包配饰" },
            { "服装服饰>男装>马甲>", "服装服饰>男装>马甲","服装服饰" },
            { "图书音像>", "图书音像", "图书音像" },
        };
    }
    
    @Test(dataProvider="data")
    public void topCategory(String input, String expectedFull, String expectedTop)
    throws Exception {
        Tuple t = Tuples.with(input);
        assertEquals(full.exec(t), expectedFull);
        assertEquals(top.exec(t), expectedTop);
    }

    @DataProvider
    public Object[][] schemas() {
        return new Object[][] {
            { "chararray",              true },
            { "long",                   false },
            { "(chararray)",            false },
            { "(long)",                 false },
            { "(chararray,long)",       false },
            { "(chararray,chararray)",  false },
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(String input, boolean valid) throws Exception {
        try {
            Schema schema = Tuples.schema(input);
            Schema output = top.outputSchema(schema);
            assertTrue(valid);
            assertEquals(output, CategoryText.SCHEMA);
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
    }
}
