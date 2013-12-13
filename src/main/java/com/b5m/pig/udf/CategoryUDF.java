package com.b5m.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

/**
 * UDF that maps a string to a product category.
 * @author Paolo D'Apice
 */
public class CategoryUDF extends EvalFunc<String> {

    @Override
    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }

        try {
            String title = (String) tuple.get(0);
            String category = null;
            // TODO code here

            return category;
        } catch (Exception e) {
            throw WrappedIOException.wrap("Caught exception while processing row", e);
        }
    }

}

