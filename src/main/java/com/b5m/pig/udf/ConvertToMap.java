package com.b5m.pig.udf;

import java.io.IOException;
import java.util.Map;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * UDF that convert a bag resulting from a GROUP BY to a map.
 * @author Paolo D'Apice
 */
public class ConvertToMap extends EvalFunc<Map> {

    @Override
    public Map exec(Tuple input) throws IOException {
        DataBag bag = (DataBag) input.get(0);
        Map<Object, Object> map = new TreeMap<Object, Object>();
        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple tuple = it.next();
            map.put(tuple.get(1), tuple.get(2));
        }
        return map;
    }

    /*
     * Expected input schema:
     * {data3: {(uuid: chararray,category: chararray,counts: long)}}
     *
     * Output schema
     * {map[]}
     */
    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 1) {
            String message = String.format("Expected input tuple of size 1, received %d",
                                           input.size());
            throw new IllegalArgumentException(message);
        }

        try {
            FieldSchema field = input.getField(0);
            if (field.type != DataType.BAG) {
                String message = String.format("Expected input (bag), received schema (%s)",
                                               DataType.findTypeName(field.type));
                throw new IllegalArgumentException(message);
            }

            Schema schema = field.schema.getField(0).schema;
            if (schema.size() != 3) {
                String message = String.format("Expected input (bag) with size 3, received %d",
                                               schema.size());
                throw new IllegalArgumentException(message);
            }

            if (schema.getField(0).type != DataType.CHARARRAY
                    || schema.getField(1).type != DataType.CHARARRAY
                    || schema.getField(2).type != DataType.LONG) {
                StringBuilder sb = new StringBuilder("Expected bag to have (chararray, chararray, long), ")
                        .append("received (").append(DataType.findTypeName(schema.getField(0).type))
                        .append(", ").append(DataType.findTypeName(schema.getField(1).type))
                        .append(", ").append(DataType.findTypeName(schema.getField(2).type))
                        .append(")");
                throw new IllegalArgumentException(sb.toString());
            }
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return new Schema(new FieldSchema(null, DataType.MAP));
    }

}

