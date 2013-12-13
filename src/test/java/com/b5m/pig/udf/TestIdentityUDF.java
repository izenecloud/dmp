package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Arrays;

public class TestIdentityUDF {

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testIdentityUDF() throws IOException {
        Tuple input = tupleFactory.newTuple(Arrays.asList("a", "b", "c"));
        IdentityUDF func = new IdentityUDF();
        Tuple output = func.exec(input);
        assertEquals(input, output);
    }

}

