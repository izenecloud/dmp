package com.b5m.pig.udf;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Extract category text from raw text from SCD.
 * 
 * @author Paolo D'Apice
 */
public class CategoryText extends EvalFunc<String> {

    final static Schema SCHEMA = new Schema(new FieldSchema("category", DataType.CHARARRAY));
    
    private final static char MARKER = '>';

    private final boolean onlyTopCategory;

    public CategoryText() {
        this("top");
    }

    public CategoryText(String mode) {
        onlyTopCategory = mode.equals("top");
    }

    @Override
    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            if (log.isDebugEnabled()) log.debug("skipping empty/null tuple");
            return null;
        }
        
        String category = (String) tuple.get(0);
        if (log.isTraceEnabled()) log.trace("category: " + category);

        if (StringUtils.isBlank(category)) {
            if (log.isDebugEnabled()) log.debug("skipping blank category");
            return null;
        }

        int i = onlyTopCategory ? category.indexOf(MARKER) : category.lastIndexOf(MARKER);
        return (i == -1) ? category : category.substring(0, i);
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 1) {
            String message = String.format("Expected input tuple of size 1, received %d",
                                           input.size());
            throw new IllegalArgumentException(message);
        }

        try {
            FieldSchema field = input.getField(0);
            if (field.type != DataType.CHARARRAY) {
                String message = String.format("Expected input (chararray), received schema (%s)",
                                               DataType.findTypeName(field.type));
                throw new IllegalArgumentException(message);
            }
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return SCHEMA;
    }
}
