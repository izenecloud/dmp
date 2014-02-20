package com.b5m.maxent;

import opennlp.maxent.ContextGenerator;
import opennlp.model.Event;

/**
 * Event generator.
 * 
 * @author Paolo D'Apice
 */
final class MaxEntEventGenerator {

    private final ContextGenerator contextGenerator = new MaxEntContextGenerator();

    /**
     * Generates a new event parsing the given text line.
     * 
     * @param line A tab-delimited text line containing with context and outcome.
     * @return A new Event.
     * @see #newEventString(java.lang.String, java.lang.String) 
     */
    Event newEvent(String line) {
        int index = line.lastIndexOf('\t');
        if (index == -1)
            return null;

        return newEvent(line.substring(index + 1), line.substring(0, index));
    }

    /**
     * Generates a new event.
     * @param outcome Event outcome.
     * @param context Event context.
     * @return A new Event.
     */
    Event newEvent(String outcome, String context) {
        return new Event(outcome, contextGenerator.getContext(context));
    }
    
    /**
     * Generates a new string containing the given context and outcome.
     * @param outcome Event outcome.
     * @param context Event context.
     * @return A new tab-delimited string with context and outcome.
     * @see #newEvent(java.lang.String)
     */
    String newEventString(String outcome, String context) {
        return String.format("%s\t%s\n", context, outcome);
    }
}
