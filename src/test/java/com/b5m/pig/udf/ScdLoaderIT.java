package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;

@Test(groups={"pig"})
public class ScdLoaderIT {

    @Test
    public void test1() throws Exception {
        String[] params = {
            "input = src/test/data/test.scd"
        };

        PigTest test = new PigTest("src/test/pig/scdLoader.pig", params);

        String[] expected = {
            "(89)"
        };

        test.assertOutput("num", expected);
    }

    @Test
    public void test2() throws Exception {
        String[] params = {
            "input = src/test/data/short.scd"
        };

        List<TestTuple> expected = Arrays.asList(
            new TestTuple(
                "e67af43226589c9399a76d85e04fac62",
                "P家经典蝴蝶结单鞋",
                "鞋包配饰>女鞋>休闲鞋>"),
            new TestTuple(
                "e67af85fdcee6555af5ae4b37550ebec",
                "潮复古民族风格修身汗背心薄款 青少年中年男士汗衫背心",
                "服装服饰>男装>马甲>"),
            new TestTuple(
                "3abc788c06e96fb8c0a333698c7fd9c5",
                "Considerations on Volcanos: The Probable Causes of Their Phenomena, "
                + "the Laws Which Determine Their March, the Disposition of Their "
                + "Products, and Their Connexion with the Present State and Past "
                + "History of the Globe",
                "图书音像>")
        );

        PigTest test = new PigTest("src/test/pig/scdLoader.pig", params);

        Iterator<Tuple> it = test.getAlias("data");
        Iterator<TestTuple> et = expected.iterator();
        while (it.hasNext()) {
            TestTuple e = et.next();
            Tuple t = it.next();

            @SuppressWarnings("unchecked")
            Map<String, String> fields = (Map<String, String>) t.get(0);
            assertEquals(fields.get("DOCID"), e.docid);
            assertEquals(fields.get("Title"), e.title);
            assertEquals(fields.get("Category"), e.category);
        }
    }

    class TestTuple {
        final String docid;
        final String title;
        final String category;
        TestTuple(String ... args) {
            this.docid = args[0];
            this.title = args[1];
            this.category = args[2];
        }
    }
}
