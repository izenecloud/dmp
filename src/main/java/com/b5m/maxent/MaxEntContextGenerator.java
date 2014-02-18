package com.b5m.maxent;

import opennlp.maxent.ContextGenerator;

/**
 * Context generator for Chinese language.
 */
final class MaxEntContextGenerator implements ContextGenerator {

    private final ChineseAnalyzer analyzer = new ChineseAnalyzer();

    @Override
    public String[] getContext(Object o) {
        String s = (String) o;
        return analyzer.tokenize(s);
    }

}

