package com.b5m.pig.udf;

import static org.testng.Assert.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pig.pigunit.PigTest;

@Test(groups={"pig"})
public class GetCategoryIT {

    private PigServer pigServer = null;

    @BeforeTest
    public void setup() throws Exception {
        pigServer = new PigServer("local");
    }

    @AfterTest
    public void teardown() {
        pigServer.shutdown();
    }

    @Test
    public void onPig() throws Exception {
        String[] args = {
            "model_file=./src/test/data/Model.txt",
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

    @Test
    public void runEmbedded() throws Exception {
        pigServer.registerJar("dist/pig-udfs.jar");
        pigServer.registerFunction("GET_CATEGORY",
                new org.apache.pig.FuncSpec("com.b5m.pig.udf.GetCategory",
                    new String[] { "src/test/data/Model.txt", "local" }
                    )
                );
        pigServer.registerQuery("titles = LOAD 'src/test/data/sample-logs.avro' AS (title:chararray);");
        pigServer.registerQuery("categories = FOREACH titles GENERATE GET_CATEGORY(title);");

        ExecJob job = pigServer.store("categories", "output", "JsonStorage");
        assertEquals(job.getStatus(), ExecJob.JOB_STATUS.COMPLETED);
        assertTrue(pigServer.existsFile("output"));

        pigServer.deleteFile("output");
        assertFalse(pigServer.existsFile("output"));
    }

    @Test
    public void loadEmbedded() throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        params.put("model_file", "src/test/data/Model.txt");
        params.put("input", "src/test/data/sample-logs.avro");

        pigServer.registerScript("src/test/pig/getCategory.pig", params);

        pigServer.setBatchOn();
        List<ExecJob> jobs = pigServer.executeBatch();
        assertEquals(jobs.size(), 1);
        assertEquals(jobs.get(0).getStatus(), ExecJob.JOB_STATUS.COMPLETED);
        assertTrue(pigServer.existsFile("output"));

        pigServer.deleteFile("output");
        assertFalse(pigServer.existsFile("output"));
    }
}

