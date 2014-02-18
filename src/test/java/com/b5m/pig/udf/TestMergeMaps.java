package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.b5m.utils.Tuples;

import java.util.Map;

@Test(groups={"pig"})
public class TestMergeMaps {

    private final static String SCHEMA = "([int])";

    private final EvalFunc<Map> func = new MergeMaps();

    @DataProvider
    public Object[][] tuples() throws Exception {
        return new Object[][] {
            {
                Tuples.withBag(
                    Tuples.fromString("([服装服饰#1])", SCHEMA),
                    Tuples.fromString("([母婴童装#1])", SCHEMA)
                ),
                Tuples.newMap("服装服饰", 1, "母婴童装", 1)
            },
            {
                Tuples.withBag(
                    Tuples.fromString("([服装服饰#1,图书音像#2])", SCHEMA),
                    Tuples.fromString("([服装服饰#3,图书音像#1,母婴童装#1])", SCHEMA)
                ),
                Tuples.newMap("服装服饰", 4, "图书音像", 3, "母婴童装", 1)
            },
        };
    }

    @Test(dataProvider="tuples")
    public void merge(Tuple input, Map expected) throws Exception {
        Map output = func.exec(input);
        assertEquals(expected, output);
    }

    @DataProvider
    public Object[][] badSchemas() {
        return new Object[][] {
            { "(chararray)" },
            { "([long])" },
            { "{([int], int)}" },
            { "{(int)}" },
        };
    }

    @Test(dataProvider="badSchemas", expectedExceptions={IllegalArgumentException.class})
    public void schemaOutputFail(String input) throws Exception {
        Schema schema = Tuples.schema(input);
        func.outputSchema(schema);
    }

    @DataProvider
    public Object[][] schemas() throws Exception {
        return new Object[][] {
            { "{([int])}" },
            { "{([long])}" },
            { "{([])}" },
            { "{([chararray])}" },
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(String input) throws Exception {
        Schema schema = Tuples.schema(input);
        Schema output = func.outputSchema(schema);
        Schema expected = Tuples.schema("[int]");
        assertEquals(output, expected);
    }

}

