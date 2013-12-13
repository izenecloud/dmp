package com.b5m.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Example UDF that returns what is passed in.
 */
public class IdentityUDF extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        return tuple;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return input;
    }

}

