package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestConvertToMap {

    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private ConvertToMap func = new ConvertToMap();

    @DataProvider(name="tuples")
    public Object[][] titles() throws ExecException {
        /*
        sample data:
        0b3d1bd97a0fcc4eb307882a2754e8c0        {(0b3d1bd97a0fcc4eb307882a2754e8c0,41,1)}
        0c6d8636f8be87e657f9b14ed07b54de        {(0c6d8636f8be87e657f9b14ed07b54de,11,1),(0c6d8636f8be87e657f9b14ed07b54de,13,1)}
         */
        return new Object[][] {
            {
                newTuple("0b3d1bd97a0fcc4eb307882a2754e8c0", "41", 1),
                newMap("41", 1)
            },
            {
                newTuple("0c6d8636f8be87e657f9b14ed07b54de", "11", 1, "13", 1),
                newMap("11", 1, "13", 1)
            }
        };
    }

    @Test(dataProvider="tuples")
    public void testConvertToMap(Tuple input, Map<Object, Integer> expected) throws IOException {
        Map output = func.exec(input);
        assertEquals(expected, output);
    }


    private Tuple newTuple(String uid, Object ... args) throws ExecException {
        DataBag bag = bagFactory.newDefaultBag();
        List<Object> list = Arrays.asList(args);
        for (Iterator<Object> it = list.iterator(); it.hasNext();) {
            String category = (String) it.next();
            Integer count = (Integer) it.next();
            Tuple tuple = tupleFactory.newTuple(3);
            tuple.set(0, uid);
            tuple.set(1, category);
            tuple.set(2, count);
            bag.add(tuple);
        }
        return tupleFactory.newTuple(bag);
    }

    private Map<Object, Integer> newMap(Object ... args) throws ExecException {
        Map<Object, Integer> map = new HashMap<Object, Integer>();
        List<Object> list = Arrays.asList(args);
        for (Iterator<Object> it = list.iterator(); it.hasNext();) {
            Object key = it.next();
            Integer value = (Integer) it.next();
            map.put(key, value);
        }
        return map;
    }

}

