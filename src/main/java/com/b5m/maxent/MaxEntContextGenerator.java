package com.b5m.maxent;

import opennlp.maxent.ContextGenerator;

public class MaxEntContextGenerator  implements ContextGenerator{

	public String[] getContext(Object o) {
		String s = (String) o;
		return ChineseAnalyzer.tokenize(s);
	}
}
