package com.b5m.pig.udf;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * UDF that merges maps.
 *
 * @author Paolo D'Apice
 */
public class MergeMaps extends EvalFunc<Map> {

    // TODO use this flag to avoid string+parsing
    //private boolean stringValues = false;

    @Override
    public Map exec(Tuple input) throws IOException {
        Map<String, Integer> outmap = new TreeMap<String, Integer>();

        DataBag bag = (DataBag) input.get(0);
        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple t = it.next();

            /*
             * At runtime there is no information about the generic types
             * but the Pig ensures that maps have String key.
             */
            @SuppressWarnings("unchecked")
            Map<String, Object> inmap = (Map<String, Object>) t.get(0);

            for (Map.Entry<String, Object> me : inmap.entrySet()) {
                String key = me.getKey();
                int value = Integer.parseInt(me.getValue().toString());

                if (outmap.containsKey(key)) {
                    int oldval = outmap.get(key);
                    outmap.put(key, oldval + value); // use an accumulator object/lambda
                } else {
                    outmap.put(key, value);
                }
            }
        }

        return outmap;
    }

    /*
     * Expected input schemas:
     * {[int]}, {[long]}, {[chararray]}, {[]}
     *
     * Output schemas:
     * [int], [long], [chararray]
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
            if (field.type != DataType.BAG) {
                String message =
                    String.format("Expected input (bag), received schema (%s)",
                                  DataType.findTypeName(field.type));
                throw new IllegalArgumentException(message);
            }

            field = field.schema.getField(0);
            Schema schema = field.schema;
            if (schema.size() != 1) {
                String message = String.format("Expected input (bag) with size 1, received %d",
                                               schema.size());
                throw new IllegalArgumentException(message);
            }

            field = field.schema.getField(0);
            if (field.type != DataType.MAP) {
                String message =
                    String.format("Expected input bag to contain (map), received schema (%s)",
                                  DataType.findTypeName(field.type));
                throw new IllegalArgumentException(message);
            }

            schema = field.schema;
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
            schema = Utils.getSchemaFromString("[int]");
        } catch (ParserException ex) {
            // this never happens
        }

        return schema;
    }

}

