package com.b5m.maxent;

import opennlp.model.EventStream;
import opennlp.model.Event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * EventStream reading from multiple files.
 * @deprecated Training is performed on Hadoop with Pig.
 *
 * @author Paolo D'Apice
 */
@Deprecated
class FilesEventStream implements EventStream {

    private final static Log log = LogFactory.getLog(FilesEventStream.class);

    private final Iterator<File> iterator;
    private final String separator;

    private FileReader fileReader;
    private TitleCategoryEventStream eventStream;

    FilesEventStream(List<File> files, String separator) throws IOException {
        this.iterator = files.iterator();
        this.separator = separator;

        if (log.isDebugEnabled()) for (File file : files) log.debug("file: " + file);

        nextFile();
    }

    private boolean nextFile() throws IOException {
        if (iterator.hasNext()) {
            if (fileReader != null) fileReader.close();

            File file = iterator.next();
            if (log.isDebugEnabled()) log.debug("streaming from file: " + file);

            fileReader = new FileReader(file);
            eventStream = new TitleCategoryEventStream(fileReader, separator);

            return true;
        }

        return false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (eventStream == null) return false;

        boolean b = eventStream.hasNext();
        if (!b && nextFile())
            return eventStream.hasNext();

        return b;
    }

    @Override
    public Event next() throws IOException {
        return eventStream.next();
    }

}

