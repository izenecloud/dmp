package com.b5m.maxent;

public class MaxEntTrainer {
	// args[0]	SCD Directory
	// args[1]  Model Directory
	public static void run(String[] args) {
    	MaxEntDataExtractor.run(args);
    	String[] maxent = new String[1];
    	maxent[0] = args[1];
    	MaxEnt.run(maxent);    
	}
	
	public static void main(String[] args) {
		run(args);
	}
}
