package com.b5m.topic.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class DocumentProcessor {
	private static final String DOC_CONTENT = "document_processor_doc_pair";
	private static final String DICT_PATH = "document_processor_dict_path";
	private static final String DOC_WORD_VECTOR = "document_processor_doc_word_vector";
	private static final String TOKENIZED_DOCUMENT = "tokenized_document";
	private static final String DF_PATH = "df";
	private static final String TF_IDF_PATH = "tf_idf";
	
	public static void setDocContentPath(Configuration conf, Path path) {
		conf.set(DOC_CONTENT, path.toString());
	}
	
	public static Path getDocumentContentPath(Configuration conf) {
		return new Path(conf.get(DOC_CONTENT));
	}
	
	public static void setDictionaryPath(Configuration conf, Path path) {
		conf.set(DICT_PATH, path.toString());
	}
	
	public static Path getDictionaryPath(Configuration conf) {
		return new Path(conf.get(DICT_PATH));
	}
	
	public static void setDocWordVectorPath(Configuration conf, Path path) {
		conf.set(DOC_WORD_VECTOR, path.toString());
	}
	
	public static Path getDocWordVectorPath(Configuration conf) {
		return new Path(conf.get(DOC_WORD_VECTOR));
	}
	
	private static final int minSupport = 5;
	private static final int minDf = 1;
	private static final int maxDFPercent = 10;
	private static final int maxNGramSize = 1;
	private static final int minLLRValue = 50;
	private static final int reduceTasks = 1;
	private static final int chunkSize = 200;
	private static final int norm = 2;
	private static final boolean sequentialAccessOutput = true;
	
	public static void preprocess(Path input, Path output, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		setDocContentPath(conf, input);
		
		Path tokenizedPath = new Path(output,TOKENIZED_DOCUMENT);
		HadoopUtil.delete(conf, tokenizedPath);
		Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_43);
		org.apache.mahout.vectorizer.DocumentProcessor.tokenizeDocuments(input, analyzer.getClass()
				.asSubclass(Analyzer.class), tokenizedPath, conf);

		Path dfDir = new Path(output, DF_PATH);
		HadoopUtil.delete(conf, dfDir);

		String tfDir = DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER;
		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
				dfDir, tfDir, conf, minSupport, maxNGramSize,
				minLLRValue, -1.0f, false, reduceTasks, chunkSize,
				sequentialAccessOutput, false);
		
		setDictionaryPath(conf, new Path(dfDir, "dictionary.file-*"));

		Path tfFolder = new Path(dfDir, tfDir);
		Path tfidfDir = new Path(output, TF_IDF_PATH);
		Pair<Long[], List<Path>> datasetFeatures = TFIDFConverter.calculateDF(
				tfFolder, tfidfDir, conf, chunkSize);

		TFIDFConverter.processTfIdf(tfFolder, tfidfDir, conf,
				datasetFeatures, minDf, maxDFPercent, norm, true,
				sequentialAccessOutput, false, reduceTasks);
		
		setDocWordVectorPath(conf, new Path(tfidfDir, "tfidf-vectors"));
	}
	
	void postprocess() {
		
	}
}
