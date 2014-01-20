package com.b5m.pig.udf;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * UDF that normalize map value to sum one.
 *
 * @author Paolo D'Apice
 */
public class NormalizeMap extends EvalFunc<Map> {

    // TODO use this flag to avoid string+parsing
    //private boolean stringValues = false;

    @Override
    public Map exec(Tuple input) throws IOException {
        /*
         * At runtime there is no information about the generic types
         * but the Pig ensures that maps have String key.
         */
        @SuppressWarnings("unchecked")
        Map<String, Object> inmap = (Map<String, Object>) input.get(0);

        // get sum of counts
        int summation = 0;
        for (Map.Entry<String, Object> me : inmap.entrySet()) {
            int value = Integer.parseInt(me.getValue().toString());
            summation += value;
        }

        // normalize to sum 1
        Map<String, Double> outmap = new TreeMap<String, Double>();
        for (Map.Entry<String, Object> me : inmap.entrySet()) {
            double value = Double.parseDouble(me.getValue().toString());
            double normalized = value / summation;
            outmap.put(me.getKey(), normalized);
        }

        return outmap;
    }

    /*
     * Expected input schema:
     * [int], [long], [chararray], []
     *
     * Output schema:
     * [double]
     */
    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 1) {
            String message =
                String.format("Expected input tuple of size 1, received %d",
                              input.size());
            throw new IllegalArgumentException(message);
        }

        try {
            FieldSchema field = input.getField(0);
            if (field.type != DataType.MAP) {
                String message =
                    String.format("Expected input (map), received schema (%s)",
                                  DataType.findTypeName(field.type));
                throw new IllegalArgumentException(message);
            }

            Schema schema = field.schema;
            if (schema != null && schema.size() == 1) {
                FieldSchema mapfield = schema.getField(0);
                if (mapfield.type != DataType.INTEGER && mapfield.type != DataType.LONG
                        && mapfield.type != DataType.CHARARRAY) {
                    String message =
                        String.format("Expected map to have [integer] or [long] or [chararray] values, got [%s]",
                                      DataType.findTypeName(mapfield.type));
                    throw new IllegalArgumentException(message);
                }
            } else {
                log.warn("Input map[] does not specify value type");
                //stringValues = true;
            }
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        Schema schema = null;
        try {
            schema = Utils.getSchemaFromString("[double]");
        } catch (ParserException ex) {
            // this never happens
        }

        return schema;
    }

}

