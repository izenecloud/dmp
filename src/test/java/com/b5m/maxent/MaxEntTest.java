package com.b5m.maxent;

import com.b5m.utils.Files;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups={"maxent"})
public class MaxEntTest {

    private final MaxEnt maxent;

    public MaxEntTest() throws IOException {
        maxent = new MaxEnt(Files.getResource("/Model.txt"));
    }

    @DataProvider
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
    public void getCategory(String title, String expected) {
        String category = maxent.getCategory(title);
        assertEquals(category, expected);
    }

}
