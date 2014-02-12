package com.b5m.maxent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.maxent.ContextGenerator;
import opennlp.model.GenericModelReader;
import opennlp.model.MaxentModel;

import java.io.File;
import java.io.IOException;

public class MaxEntCategoryClassifier implements CategoryClassifier {

    private static final Logger log = LoggerFactory.getLogger(MaxEntCategoryClassifier.class);

    private final MaxentModel model;
    private final ContextGenerator contextGenerator;

    public MaxEntCategoryClassifier(File modelFile) throws IOException {
        log.info("Loading model from: " +  modelFile);
        model = new GenericModelReader(modelFile).getModel();
        contextGenerator = new MaxEntContextGenerator();
    }

    @Override
    public String getCategory(String title) {
        String[] context = contextGenerator.getContext(title);
        return eval(context);
    }

    String eval(String[] context) {
        double[] outcome = model.eval(context);
        return model.getBestOutcome(outcome);
    }

}

