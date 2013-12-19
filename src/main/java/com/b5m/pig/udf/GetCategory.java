package com.b5m.pig.udf;

import com.b5m.maxent.CategoryClassifier;
import com.b5m.maxent.MaxEnt;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * UDF that maps a string to a product category.
 * @author Paolo D'Apice
 */
public class GetCategory extends EvalFunc<String> {

    private final String filename;
    private CategoryClassifier classifier = null;

    public GetCategory(String filename) {
        this.filename = filename;
    }

    @Override
    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }

        String title = (String) tuple.get(0);

        if (classifier == null) // TODO move to constructor?
            classifier = new MaxEnt(new File(MaxEnt.MODEL_FILENAME));

        return classifier.getCategory(title);
    }

    @Override
    public List<String> getCacheFiles() {
        String name = String.format("%s#%s", filename, MaxEnt.MODEL_FILENAME);
        return Arrays.asList(name);
    }

    /* for tests only */
    GetCategory(File file) throws IOException {
        filename = file.toString();
        classifier = new MaxEnt(file);
    }

}

