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

    public Event newEvent(String outcome, String context) {
        return new Event(outcome, contextGenerator.getContext(context));
    }
    
}
