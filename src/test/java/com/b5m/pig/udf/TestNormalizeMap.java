package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.b5m.utils.Tuples;

import java.util.Map;

public class TestNormalizeMap {

    private NormalizeMap func = new NormalizeMap();

    @DataProvider
    public Object[][] tuples() throws Exception {
        return new Object[][] {
            {
                Tuples.newTuple(Tuples.newMap("服装服饰", 1)),
                Tuples.newMap("服装服饰", 1.0)
            },
            {
                Tuples.newTuple(Tuples.newMap("服装服饰", "1")),
                Tuples.newMap("服装服饰", 1.0)
            },
            {
                Tuples.newTuple(Tuples.newMap("服装服饰", 1, "图书音像", 2, "母婴童装", 1)),
                Tuples.newMap("服装服饰", 0.25, "图书音像", 0.5, "母婴童装", 0.25)
            },
            {
                Tuples.newTuple(Tuples.newMap("服装服饰", "1", "图书音像", "2", "母婴童装", "1")),
                Tuples.newMap("服装服饰", 0.25, "图书音像", 0.5, "母婴童装", 0.25)
            }
        };
    }

    @Test(dataProvider="tuples")
    public void normalize(Tuple input, Map<Object, Double> expected) throws Exception {
        Map output = func.exec(input);
        assertEquals(expected, output);
    }

    @DataProvider
    public Object[][] badSchemas() throws Exception {
        return new Object[][] {
            { Tuples.schema("(chararray)") },
            { Tuples.schema("(chararray, int)") },
            { Tuples.schema("([long])") },
            { Tuples.schema("([chararray])") },
        };
    }

    @Test(dataProvider="badSchemas", expectedExceptions={IllegalArgumentException.class})
    public void schemaOutputFail(Schema input) throws Exception {
        func.outputSchema(input);
    }

    @DataProvider
    public Object[][] schemas() throws Exception {
        return new Object[][] {
            { Tuples.schema("[int]") },
            { Tuples.schema("[long]") },
            { Tuples.schema("[]") },
            { Tuples.schema("[chararray]") },
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(Schema input) throws Exception {
        Schema output = func.outputSchema(input);
        Schema expected = Tuples.schema("[double]");
        assertEquals(output, expected);
    }

}

