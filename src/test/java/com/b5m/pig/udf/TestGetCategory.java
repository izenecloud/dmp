package com.b5m.pig.udf;

import com.b5m.utils.Files;
import com.b5m.utils.Tuples;

import static org.testng.Assert.*;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.File;

@Test(groups={"pig"})
public class TestGetCategory {

    private EvalFunc<String> func;

    @BeforeTest
    public void setUp() throws Exception {
        File file = Files.getResource("/Model.txt");
        func = new GetCategory(file.toString(), "local");
    }

    @DataProvider
    public Object[][] titles() {
        return new Object[][] {
            {
                "蔻玲2013冬新款女狐狸毛领羊绒呢子短款大衣寇玲原价1999专柜正品",
                "服装服饰"
            },
            {
                "深部条带煤柱长期稳定性基础实验研究 正版包邮",
                "图书音像"
            },
        };
    }

    @Test(dataProvider="titles")
    public void test(String title, String category) throws Exception {
        Tuple tuple = Tuples.with(title);
        String output = func.exec(tuple);
        assertEquals(output, category);
    }

    @DataProvider
    public Object[][] schemas() {
        return new Object[][] {
            { "chararray",              true },
            { "category: chararray",    true },
            { "long",                   false },
            { "(chararray)",            false },
            { "(long)",                 false },
            { "(chararray,long)",       false },
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(String input, boolean valid) throws Exception {
        try {
            Schema schema = Tuples.schema(input);
            Schema output = func.outputSchema(schema);
            assertTrue(valid);
            assertEquals(output, GetCategory.SCHEMA);
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
    }

}

