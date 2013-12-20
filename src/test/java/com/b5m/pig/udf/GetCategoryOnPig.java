package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

import java.io.IOException;

public class GetCategoryOnPig {

    @Test
    public void test() throws IOException, ParseException {
        String[] args = {
            "model_file=./src/test/resources/Model.txt",
        };

        PigTest test = new PigTest("./src/test/pig/getCategory.pig", args);

        String[] input = {
            "蔻玲2013冬新款女狐狸毛领羊绒呢子短款大衣寇玲原价1999专柜正品",
            "深部条带煤柱长期稳定性基础实验研究 正版包邮",
        };

        String[] output = {
             "(服装服饰)",
             "(图书音像)",
        };

        test.assertOutput(
            "titles", input,
            "categories", output
        );
    }

}

