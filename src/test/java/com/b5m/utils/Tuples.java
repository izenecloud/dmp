package com.b5m.utils;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utility methods for building test tuples and schemas.
 * @author Paolo D'Apice
 */
public final class Tuples {

    private final static TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     * Creates a new map with given key-value pairs.
     */
    public static <Value> Map<String, Value> newMap(Object ... args) {
        Map<String, Value> map = new TreeMap<String, Value>();
        List<Object> list = Arrays.asList(args);
        for (Iterator<Object> it = list.iterator(); it.hasNext();) {
            String key = (String) it.next();
            /*
             * Value is the template parameter so it's safe to cast to.
             */
            @SuppressWarnings("unchecked")
            Value value = (Value) it.next();
            map.put(key, value);
        }
        return map;
    }

    /**
     * Creates a new Tuple with given values.
     */
    public static Tuple with(Object ... args) {
        return tupleFactory.newTuple(Arrays.asList(args));
    }

    /**
     * Creates a new Schema.
     */
    public static Schema schema(String string) throws Exception {
        return Utils.getSchemaFromString(string);
    }

    /**
     * Creates a new ResourceSchema.
     */
    public static ResourceSchema resourceSchema(String string) throws Exception {
        return new ResourceSchema(schema(string));
    }

    public static Tuple fromString(String tupleString, String schemaString) throws Exception {
        LogicalSchema schema = Utils.parseSchema(schemaString);
        Utf8StorageConverter converter = new Utf8StorageConverter();
        ResourceFieldSchema fieldSchema = new ResourceFieldSchema(schema.getField(0));
        Tuple tuple = converter.bytesToTuple(tupleString.getBytes("UTF-8"), fieldSchema);
        return tuple;
    }

    // prevents instantiation
    private Tuples() {}

}

