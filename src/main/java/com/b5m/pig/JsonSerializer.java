package com.b5m.pig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Serialize Tuple to Json.
 * @author Paolo D'Apice
 */
public class JsonSerializer {

    private final static Log log = LogFactory.getLog(JsonSerializer.class);

    // Default size for the byte buffer, should fit most tuples.
    private final static int BUF_SIZE = 4 * 1024;

    private final JsonFactory jsonFactory;

    public JsonSerializer() {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, false);
    }

    /**
     * Serialize the input tuple into a JSON string.
     * @param tuple Input tuple.
     * @param fields Input schema.
     * @return Json string.
     * @throws IOException if serialization errors occur.
     */
    public String toJson(Tuple tuple, ResourceFieldSchema[] fields)
    throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("input : " + tuple);
            log.debug("schema: " + Arrays.toString(fields));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUF_SIZE);
        JsonGenerator json = jsonFactory.createJsonGenerator(baos, JsonEncoding.UTF8);

        json.writeStartObject();
        for (int i = 0; i < fields.length; i++) {
            writeField(json, fields[i], tuple.get(i));
        }
        json.writeEndObject();
        json.close();

        if (log.isDebugEnabled()) log.debug("serialized: " + baos);

        return baos.toString();
    }

    /*
     * Write field to json.
     */
    private void writeField(JsonGenerator json, ResourceFieldSchema field, Object value)
    throws IOException {
        if (value == null) {
            json.writeNullField(field.getName());
            return;
        }

        // Based on the field's type, write it out
        switch (field.getType()) {
        case DataType.INTEGER:
            json.writeNumberField(field.getName(), (Integer) value);
            return;

        case DataType.LONG:
            json.writeNumberField(field.getName(), (Long) value);
            return;

        case DataType.FLOAT:
            json.writeNumberField(field.getName(), (Float) value);
            return;

        case DataType.DOUBLE:
            json.writeNumberField(field.getName(), (Double) value);
            return;

        case DataType.BYTEARRAY:
            json.writeBinaryField(field.getName(), ((DataByteArray) value).get());
            return;

        case DataType.CHARARRAY:
            json.writeStringField(field.getName(), (String) value);
            return;

        case DataType.MAP:
            json.writeFieldName(field.getName());
            json.writeStartObject();

            ResourceSchema s = field.getSchema();
            boolean hasValueSchema = (s != null);
            ResourceFieldSchema[] fs = hasValueSchema ? s.getFields() : null;

            /*
             * Pig supports only maps with keys of chararray data type,
             * so here it is safe to cast to Map<String, Object>.
             */
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            for (Map.Entry<String, Object> e : map.entrySet()) {
                if (hasValueSchema) {
                    writeField(json, fs[0].setName(e.getKey()), e.getValue());
                } else {
                    // no map value data type, defaults to string
                    json.writeStringField(e.getKey(), e.getValue().toString());
                }
            }
            json.writeEndObject();
            return;

        case DataType.TUPLE:
            json.writeFieldName(field.getName());
            json.writeStartObject();

            s = field.getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                        + "this storage function. No schema found for field " +
                        field.getName());
            }
            fs = s.getFields();

            Tuple tuple = (Tuple) value;
            for (int j = 0; j < fs.length; j++) {
                writeField(json, fs[j], tuple.get(j));
            }

            json.writeEndObject();
            return;

        case DataType.BAG:
            json.writeFieldName(field.getName());
            json.writeStartArray();

            s = field.getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                        + "this storage function. No schema found for field " +
                        field.getName());
            }

            fs = s.getFields();
            if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                throw new IOException("Found a bag without a tuple "
                        + "inside!");
            }

            // Drill down the next level to the tuple's schema.
            s = fs[0].getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                        + "this storage function. No schema found for field " +
                        field.getName());
            }

            fs = s.getFields();
            for (Tuple t : (DataBag) value) {
                json.writeStartObject();
                for (int j = 0; j < fs.length; j++) {
                    writeField(json, fs[j], t.get(j));
                }
                json.writeEndObject();
            }

            json.writeEndArray();
        }
    }

}

