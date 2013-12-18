package com.b5m.maxent;

import java.io.IOException;
import opennlp.maxent.ContextGenerator;
import opennlp.maxent.DataStream;
import opennlp.model.AbstractEventStream;
import opennlp.model.Event;

class MaxEntEventStream extends AbstractEventStream {
    final static String DEFAULT_SEPARATOR = " ";

    private final DataStream dataStream;
    private final String separator;
    private final ContextGenerator cg;

    private Event next;

    public MaxEntEventStream(DataStream dataStream, String separator) {
        this.separator = separator;
        this.cg = new MaxEntContextGenerator();
        this.dataStream = dataStream;

        if (dataStream.hasNext())
            next = createEvent((String) dataStream.nextToken());
    }

    public MaxEntEventStream(DataStream dataStream) {
        this(dataStream, DEFAULT_SEPARATOR);
    }

    @Override
    public boolean hasNext() throws IOException {
        while (next == null && dataStream.hasNext())
            next = createEvent((String) dataStream.nextToken());
        return next != null;
    }

    @Override
    public Event next() throws IOException {
        while (next == null && dataStream.hasNext())
            next = createEvent((String) dataStream.nextToken());

        Event current = next;
        if (dataStream.hasNext()) {
            next = createEvent((String) dataStream.nextToken());
        } else {
            next = null;
        }
        return current;
    }

    private Event createEvent(String obs) {
        int lastSpace = obs.lastIndexOf(separator);
        if (lastSpace == -1)
            return null;

        return new Event(obs.substring(lastSpace + 1),
                         cg.getContext(obs.substring(0, lastSpace)));
    }

}
