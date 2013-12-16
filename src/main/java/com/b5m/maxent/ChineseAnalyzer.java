package com.b5m.maxent;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

//import com.google.common.io.Closeables;

public class ChineseAnalyzer {
	static private Analyzer analyzer = new SmartChineseAnalyzer(
			Version.LUCENE_43);

	static String[] tokenize(String document) {
		try {
			TokenStream tokenStream = analyzer.tokenStream(new String("field"),
					new StringReader(document));

			List<String> tokens = new ArrayList<String>();
			CharTermAttribute token = tokenStream
					.addAttribute(CharTermAttribute.class);

			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				tokens.add(token.toString());
			}
			tokenStream.end();
//			Closeables.close(tokenStream, true);
			tokenStream.close();
			return tokens.toArray(new String[0]);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
