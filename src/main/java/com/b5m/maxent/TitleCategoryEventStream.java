package com.b5m.maxent;

import opennlp.maxent.ContextGenerator;
import opennlp.maxent.DataStream;
import opennlp.maxent.PlainTextByLineDataStream;
import opennlp.model.AbstractEventStream;
import opennlp.model.Event;

import java.io.IOException;
import java.io.FileReader;

/**
 * EventStream reading title-category pairs from file.
 * @deprecated Training is performed on Hadoop with Pig.
 * 
 * @author Paolo D'Apice
 */
@Deprecated
final class TitleCategoryEventStream extends AbstractEventStream {

    private final DataStream dataStream;
    private final String separator;
    private final ContextGenerator contextGenerator;

    private Event next;

    TitleCategoryEventStream(FileReader reader, String separator) {
        this.dataStream = new PlainTextByLineDataStream(reader);
        this.separator = separator;
        this.contextGenerator = new MaxEntContextGenerator();

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
        int index = token.lastIndexOf(separator);

        if (index == -1)
            return null;

        return new Event(token.substring(index + 1),
                         contextGenerator.getContext(token.substring(0, index)));
    }

}
