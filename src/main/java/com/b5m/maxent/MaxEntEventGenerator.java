package com.b5m.maxent;

import opennlp.maxent.ContextGenerator;
import opennlp.model.Event;

/**
 * Event generator.
 * 
 * @author Paolo D'Apice
 */
public final class MaxEntEventGenerator {

    private final ContextGenerator contextGenerator = new MaxEntContextGenerator();

    public Event newEvent(String line) {
        int index = line.lastIndexOf('\t');
        if (index == -1)
            return null;

        return newEvent(line.substring(index + 1), line.substring(0, index));
    }

    public Event newEvent(String outcome, String context) {
        return new Event(outcome, contextGenerator.getContext(context));
    }
    
    public String newEventString(String outcome, String context) {
        return String.format("%s\t%s\n", context, outcome);
    }
}
