package com.b5m.maxent;

import opennlp.maxent.DataStream;
import opennlp.maxent.PlainTextByLineDataStream;
import opennlp.model.Event;
import opennlp.model.EventStream;

import java.io.IOException;
import java.io.Reader;

/**
 * EventStream reading from single file.
 * 
 * @author Paolo D'Apice
 */
final class FileEventStream implements EventStream {

    private final MaxEntEventGenerator generator;
    private final DataStream dataStream;

    private Event next;

    FileEventStream(Reader reader) {
        this.generator = new MaxEntEventGenerator();
        this.dataStream = new PlainTextByLineDataStream(reader);

        if (dataStream.hasNext())
            next = createEvent();
    }

    @Override
    public boolean hasNext() throws IOException {
        while (next == null && dataStream.hasNext())
            next = createEvent();
        return next != null;
    }

    @Override
    public Event next() throws IOException {
        while (next == null && dataStream.hasNext())
            next = createEvent();

        Event current = next;
        next = dataStream.hasNext() ? createEvent() : null;
        return current;
    }

    private Event createEvent() {
        String token = (String) dataStream.nextToken();
        return generator.newEvent(token);
    }

}
