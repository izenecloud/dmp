package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import static org.apache.pig.impl.logicalLayer.schema.SchemaUtil.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TestConvertToMap {

    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private ConvertToMap func = new ConvertToMap();
    private Schema schema = new Schema(new FieldSchema(null, DataType.MAP));

    @DataProvider
    public Object[][] tuples() throws Exception {
        /*
        sample data:
        0b3d1bd97a0fcc4eb307882a2754e8c0        {(0b3d1bd97a0fcc4eb307882a2754e8c0,服装服饰,1)}
        0c6d8636f8be87e657f9b14ed07b54de        {(0c6d8636f8be87e657f9b14ed07b54de,服装服饰,1),(0c6d8636f8be87e657f9b14ed07b54de,图书音像,2),(0c6d8636f8be87e657f9b14ed07b54de,母婴童装,1)}
         */
        return new Object[][] {
            {
                newTuple("0b3d1bd97a0fcc4eb307882a2754e8c0", "服装服饰", 1),
                newMap("服装服饰", 1)
            },
            {
                newTuple("0c6d8636f8be87e657f9b14ed07b54de", "服装服饰", 1, "图书音像", 2, "母婴童装", 1),
                newMap("服装服饰", 1, "图书音像", 2, "母婴童装", 1)
            }
        };
    }

    @Test(dataProvider="tuples")
    public void convertToMap(Tuple input, Map<Object, Integer> expected) throws IOException {
        Map output = func.exec(input);
        assertEquals(expected, output);
    }

    @DataProvider
    public Object[][] badSchemas() throws Exception {
        return new Object[][] {
            { newTupleSchema(Arrays.asList(DataType.CHARARRAY)) },
            { newTupleSchema(Arrays.asList(DataType.CHARARRAY, DataType.CHARARRAY)) },
            { newBagSchema(Arrays.asList(DataType.CHARARRAY, DataType.CHARARRAY, DataType.INTEGER)) },
        };
    }

    @Test(dataProvider="badSchemas", expectedExceptions={IllegalArgumentException.class})
    public void schemaOutputFail(Schema input) throws IOException {
        func.outputSchema(input);
    }

    @DataProvider
    public Object[][] schemas() throws Exception {
        /* {data3: {(uuid: chararray,category: chararray,counts: long)}} */
        return new Object[][] {
            { newBagSchema("data3", null,
                    Arrays.asList("uuid", "category", "counts"),
                    Arrays.asList(DataType.CHARARRAY, DataType.CHARARRAY, DataType.LONG)) },
        };
    }

    @Test(dataProvider="schemas")
    public void schemaOutput(Schema input) throws IOException {
        Schema output = func.outputSchema(input);
        assertEquals(output, schema);
    }

    private Tuple newTuple(String uid, Object ... args) throws Exception {
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

    private Map<Object, Integer> newMap(Object ... args) throws Exception {
        Map<Object, Integer> map = new TreeMap<Object, Integer>();
        List<Object> list = Arrays.asList(args);
        for (Iterator<Object> it = list.iterator(); it.hasNext();) {
            Object key = it.next();
            Integer value = (Integer) it.next();
            map.put(key, value);
        }
        return map;
    }

}

