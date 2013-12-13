package com.b5m.pig.udf;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class ChineseAnalyzer {
	static private Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_43);
	static String[] tokenize(String document) {
		TokenStream tokenStream = null;
		try {
			tokenStream = analyzer.tokenStream(new String("field"),
					new StringReader(document));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		List<String> tokens = new ArrayList<String>();
		CharTermAttribute token = tokenStream.addAttribute(CharTermAttribute.class);
		try {
			tokenStream.reset();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		try {
			while (tokenStream.incrementToken()) {
				tokens.add(token.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return tokens.toArray(new String[0]);
	}
}
