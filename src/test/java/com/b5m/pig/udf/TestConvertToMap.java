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
public class TestConvertToMap {

    private final static String SCHEMA = "({(uuid:chararray, category:chararray, count:int)})";

    private final EvalFunc<Map> func = new ConvertToMap();

    @DataProvider
    public Object[][] tuples() throws Exception {
        return new Object[][] {
            {
                Tuples.fromString("({(0b3d1bd97a0fcc4eb307882a2754e8c0,服装服饰,1)})", SCHEMA),
                Tuples.newMap("服装服饰", 1)
            },
            {
                Tuples.fromString("({(0c6d8636f8be87e657f9b14ed07b54de,服装服饰,1),"
                                  + "(0c6d8636f8be87e657f9b14ed07b54de,图书音像,2),"
                                  + "(0c6d8636f8be87e657f9b14ed07b54de,母婴童装,1)})", SCHEMA),
                Tuples.newMap("服装服饰", 1, "图书音像", 2, "母婴童装", 1)
            }
        };
    }

    @Test(dataProvider="tuples")
    public void convertToMap(Tuple input, Map<Object, Integer> expected) throws Exception {
        Map output = func.exec(input);
        assertEquals(expected, output);
    }

    @DataProvider
    public Object[][] badSchemas() throws Exception {
        return new Object[][] {
            { Tuples.schema("(chararray)") },
            { Tuples.schema("{(chararray)}") },
            { Tuples.schema("{(chararray, chararray)}") },
            { Tuples.schema("{(chararray, chararray, int)}") },
        };
    }

    @Test(dataProvider="badSchemas", expectedExceptions={IllegalArgumentException.class})
    public void schemaOutputFail(Schema input) throws Exception {
        func.outputSchema(input);
    }

    @DataProvider
    public Object[][] schemas() throws Exception {
        return new Object[][] {
            { Tuples.schema("{(uuid:chararray, category:chararray, counts:long)}") }
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(Schema input) throws Exception {
        Schema output = func.outputSchema(input);
        Schema expected = Tuples.schema("[int]");
        assertEquals(output, expected);
    }

}

