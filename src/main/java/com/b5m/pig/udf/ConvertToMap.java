package com.b5m.pig.udf;

import java.io.IOException;
import java.util.Map;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

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

}

