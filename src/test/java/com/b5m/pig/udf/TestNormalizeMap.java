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
public class TestNormalizeMap {

    private final EvalFunc<Map> func = new NormalizeMap();

    @DataProvider
    public Object[][] tuples() throws Exception {
        return new Object[][] {
            {
                Tuples.with(Tuples.<Integer>newMap("服装服饰", 1)),
                Tuples.newMap("服装服饰", 1.0)
            },
            {
                Tuples.with(Tuples.<String>newMap("服装服饰", "1")),
                Tuples.newMap("服装服饰", 1.0)
            },
            {
                Tuples.with(Tuples.<Integer>newMap("服装服饰", 1, "图书音像", 2, "母婴童装", 1)),
                Tuples.newMap("服装服饰", 0.25, "图书音像", 0.5, "母婴童装", 0.25)
            },
            {
                Tuples.with(Tuples.<String>newMap("服装服饰", "1", "图书音像", "2", "母婴童装", "1")),
                Tuples.newMap("服装服饰", 0.25, "图书音像", 0.5, "母婴童装", 0.25)
            }
        };
    }

    @Test(dataProvider="tuples")
    public void normalize(Tuple input, Map expected) throws Exception {
        Map output = func.exec(input);
        assertEquals(expected, output);
    }

    @DataProvider
    public Object[][] badSchemas() {
        return new Object[][] {
            { "(chararray)" },
            { "(chararray, int)" },
            { "([long])" },
            { "([chararray])" },
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
            { "[int]" },
            { "[long]" },
            { "[]" },
            { "[chararray]" },
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(String input) throws Exception {
        Schema schema = Tuples.schema(input);
        Schema output = func.outputSchema(schema);
        Schema expected = Tuples.schema("[double]");
        assertEquals(output, expected);
    }

}

