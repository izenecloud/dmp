package com.b5m.utils;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.data.Tuple;

import java.util.Map;

public class TestTuples {

    @Test
    public void fromString1() throws Exception {
        String in = "(aaaabbbbcccc,4)";
        String s = "(chararray,int)";

        Tuple t = Tuples.fromString(in,s);

        assertEquals(2, t.size());
        assertEquals((String) t.get(0), "aaaabbbbcccc");
        assertEquals((Integer) t.get(1), Integer.valueOf(4));
    }

    @Test
    public void fromString2() throws Exception {
        String in = "(aaaabbbbcccc,[a#1,b#2])";
        String s = "(chararray,[int])";

        Tuple t = Tuples.fromString(in,s);

        assertEquals(2, t.size());
        assertEquals((String) t.get(0), "aaaabbbbcccc");
        @SuppressWarnings("unchecked")
        Map<String, Integer> m = (Map<String, Integer>) t.get(1);
        assertEquals(m.size(), 2);
        assertEquals(m.get("a"), Integer.valueOf(1));
        assertEquals(m.get("b"), Integer.valueOf(2));
    }

}
