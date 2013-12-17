package com.b5m.maxent;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestMaxEnt {

    private MaxEnt maxent;

    public TestMaxEnt() throws IOException {
        String modelFile = "src/test/resources/Model.txt";
        maxent = new MaxEnt(modelFile);
    }

    @DataProvider(name="titles")
    public Object[][] titles() {
        return new Object[][] {
            // input title, output category
            { "蔻玲2013冬新款女狐狸毛领羊绒呢子短款大衣寇玲原价1999专柜正品", "服装服饰" },
            { "深部条带煤柱长期稳定性基础实验研究 正版包邮", "图书音像" },
            // TODO { "", "" },
            // TODO { null, "" },
        };
    }

    @Test(dataProvider="titles")
    public void testMaxEnt(String title, String expected) {
        String category = maxent.eval(title);
        assertEquals(category, expected);
    }

}
