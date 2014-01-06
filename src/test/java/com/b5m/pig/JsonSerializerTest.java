package com.b5m.pig;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;

import com.b5m.utils.Tuples;

public class JsonSerializerTest {

    private final JsonSerializer serializer = new JsonSerializer();

    @DataProvider
    public Object[][] tuples() throws Exception {
        return new Object[][] {
            {
                Tuples.newTuple("uuid", "value"),
                Tuples.resourceSchema("key:chararray, value:chararray"),
                "{\"key\":\"uuid\",\"value\":\"value\"}"
            },
            {
                Tuples.newTuple("uuid", 42),
                Tuples.resourceSchema("foo:chararray, bar:int"),
                "{\"foo\":\"uuid\",\"bar\":42}"
            },
            {
                Tuples.newTuple("0123456789abcde", Tuples.newMap("cat1",1,"cat2",2)),
                Tuples.resourceSchema("uuid:chararray, categories:map[int]"),
                "{\"uuid\":\"0123456789abcde\",\"categories\":{\"cat1\":1,\"cat2\":2}}"
            },
            {
                Tuples.newTuple("0123456789abcde", Tuples.newMap("cat1","1","cat2","2")),
                Tuples.resourceSchema("uuid:chararray, categories:map[chararray]"),
                "{\"uuid\":\"0123456789abcde\",\"categories\":{\"cat1\":\"1\",\"cat2\":\"2\"}}"
            },
            {
                Tuples.newTuple("0123456789abcde", Tuples.newMap("cat1","1","cat2","2")),
                Tuples.resourceSchema("uuid:chararray, categories:map[]"),
                "{\"uuid\":\"0123456789abcde\",\"categories\":{\"cat1\":\"1\",\"cat2\":\"2\"}}"
            },
        };
    }

    @Test(dataProvider="tuples")
    public void testSerialization(Tuple input, ResourceSchema schema,  String expected)
    throws Exception {
        String json = serializer.toJson(input, schema.getFields());
        assertEquals(json, expected);
    }

}
