package com.b5m.maxent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import opennlp.maxent.ContextGenerator;
import opennlp.model.GenericModelReader;
import opennlp.model.MaxentModel;

import java.io.File;
import java.io.IOException;

public class MaxEntCategoryClassifier implements CategoryClassifier {

    private static final Log log = LogFactory.getLog(MaxEntCategoryClassifier.class);

    private final MaxentModel model;
    private final ContextGenerator contextGenerator;

    public MaxEntCategoryClassifier(File modelFile) throws IOException {
        if (log.isInfoEnabled()) log.info("Loading model from: " +  modelFile);

        model = new GenericModelReader(modelFile).getModel();
        contextGenerator = new MaxEntContextGenerator();

        if (log.isInfoEnabled()) log.info("Model loaded");
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

