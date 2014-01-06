package com.b5m.gson;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.gson.Gson;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TestJsonSerialization {

    private Gson gson = new Gson();

    @DataProvider(name = "data")
    public Object[][] jsons() {
        return new Object[][] {
            {
                "{\"string\":\"value\",\"map\":{\"key1\":1,\"key2\":2}}",
                new Dummy("value", "key1", 1, "key2", 2),
                Dummy.class
            }
        };
    }

    @Test(dataProvider = "data")
    public void fromJson(String json, Object expected, Class<?> clazz) {
        Object object = gson.fromJson(json, clazz);
        assertEquals(object, expected);
    }

    @Test(dataProvider = "data")
    public void toJson(String expected, Object object, Class<?> clazz) {
        String json = gson.toJson(object);
        assertEquals(json, expected);
    }

}

class Dummy {
    private String string;
    private Map<String, Integer> map = new TreeMap<String, Integer>();

    Dummy(String s, Object ... kv) {
        string = s;
        List<Object> list = Arrays.asList(kv);
        for (Iterator<Object> it = list.iterator(); it.hasNext();) {
            String k = (String) it.next();
            Integer v = (Integer) it.next();
            map.put(k, v);
        }
    }

    @Override
    public boolean equals(Object o) {
        Dummy d = (Dummy) o;
        return string.equals(d.string) && map.equals(d.map);
    }
}
