package com.b5m.pig.udf;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Filter tuples suitable for training a Maximum Entropy classifier.
 * 
 * @author Paolo D'Apice
 */
public class MaxEntPairs extends FilterFunc {

    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return Boolean.FALSE;
        }

        String first = (String) tuple.get(0);
        String second = (String) tuple.get(1);

        boolean invalid = StringUtils.isBlank(first)
                || StringUtils.isBlank(second);
        return !invalid;
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 2) {
            String message = String.format("Expected input tuple of size 2, received %d",
                                           input.size());
            throw new IllegalArgumentException(message);
        }

        try {
            FieldSchema field1 = input.getField(0);
            FieldSchema field2 = input.getField(1);
            if (field1.type != DataType.CHARARRAY || field2.type != DataType.CHARARRAY) {
                String message = String.format("Expected input (chararray,chararray), received schema (%s,%s)",
                                               DataType.findTypeName(field1.type),
                                               DataType.findTypeName(field2.type));
                throw new IllegalArgumentException(message);
            }
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

}
