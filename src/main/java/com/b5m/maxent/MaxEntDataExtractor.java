package com.b5m.maxent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxEntDataExtractor {
	private static final Logger log = LoggerFactory.getLogger(MaxEntDataExtractor.class);
	
	static class MaxEntThread extends Thread {
		private String scdFile;
		private String output;
		private MaxEntDataExtractor extractor;
		MaxEntThread(String scd, String out) {
			scdFile = scd;
			output = out; 
			extractor = new MaxEntDataExtractor();
         }

         public void run() {
        	 extractor.extract(scdFile, output);
         }
	}
	
	private String title = null;
	private String category = null;
	private boolean firstDocument = true;

	void extract(String scdFile, String output) {
		String trainDir = output;
		File file = new File(scdFile);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			new File(trainDir).mkdirs();
			String trainFile = trainDir + file.getName();
			log.debug(trainFile);
			File tFile = new File(trainFile);
			tFile.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(tFile));

			while (nextDocument(reader)) {
				if (title.isEmpty() || category.isEmpty()) {
					continue;
				}
				int topLevelIndex = category.indexOf('>');
				if (-1 == topLevelIndex)
					writer.write(title + " " + category + "\n");
				else
					writer.write(title + " "
							+ category.substring(0, topLevelIndex) + "\n");
			}
			writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	boolean nextDocument(BufferedReader reader) {
		title = new String();
		category = new String();
		boolean hasNext = false;
		try {
			String line = null;
			while (null != (line = reader.readLine())) {
				if (line.startsWith("<DOCID>")) {
					if (firstDocument) {
						firstDocument = false;
						continue;
					}
					hasNext = true;
					break;
				} else if (line.startsWith("<Title>")) {
					title = line.substring(7);
				} else if (line.startsWith("<Category>")) {
					category = line.substring(10);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return hasNext;
	}

	static void run(String[] args) {
		if (args.length != 2)
			return;
		String scdDir = args[0];
		String trainDir = args[1] + "/train/";
		String testDir = args[1] + "/test/";
		File directory = new File(scdDir);

		File[] fList = directory.listFiles();
		MaxEntThread[] threadGroup = new MaxEntThread[fList.length];
		
		int trainFiles = (int) (fList.length * 0.3) + 1;
		int trainedFiles = 0;
		for (File file : fList) {
			if (file.isFile()) {
				if (trainedFiles < trainFiles) {
					threadGroup[trainedFiles] = new MaxEntThread(file.getAbsolutePath(), trainDir);
				}
				else {
					threadGroup[trainedFiles] = new MaxEntThread(file.getAbsolutePath(), testDir);
				}
				trainedFiles++;
			}
		}
		
		log.info("Extracting Tainning Data and Test Data from SCD Files...");
		for (MaxEntThread thread : threadGroup) {
			thread.start();
		}
		
		
		for (MaxEntThread thread : threadGroup) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		log.info("Extract Tainning Data and Test Data from SCD Files FINISHED");
	}
}
