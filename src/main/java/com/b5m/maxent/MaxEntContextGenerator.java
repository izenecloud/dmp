package com.b5m.maxent;

import opennlp.maxent.ContextGenerator;

class MaxEntContextGenerator implements ContextGenerator {

    private ChineseAnalyzer analyzer = new ChineseAnalyzer();

    public String[] getContext(Object o) {
        String s = (String) o;
        return analyzer.tokenize(s);
    }

}

