package com.b5m.scd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Paolo D'Apice
 */
class ScdFileReader implements Closeable {

    private final static Log log = LogFactory.getLog(ScdFileReader.class);

    final static String DOCID_TAG = "<DOCID>";

    private BufferedReader reader;
    
    ScdFileReader(InputStream is) {
        reader = new BufferedReader(new InputStreamReader(is));
    }

    boolean hasNext() throws IOException {
        return nextDocument();
    }

    Document next() {
        return current;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private Document current;
    private Document next;

    private boolean nextDocument() throws IOException {
        String line = null;
        while (null != (line = reader.readLine())) {
            if (StringUtils.isBlank(line)) continue;
            
            Entry e = parse(line);
            if (e.tag.equals(DOCID_TAG)) {
                if (next == null) {
                    if (log.isTraceEnabled()) 
                        log.trace("found first document: " + e);

                    next = new Document();
                    next.add(e);

                    continue;
                }

                if (log.isTraceEnabled())
                    log.trace("found new document: " + e);

                current = next;
                
                next = new Document();
                next.add(e);

                return true;
            }

            if (log.isTraceEnabled())
                log.trace("add entry: " + e.tag);

             next.add(e);
        }
        
        if (log.isTraceEnabled())
            log.trace("finished to read file");
        
        // still one document to return
        if (next != null) {
            current = next;
            next = null;

            return true;
        }

        return false;
    }

    Entry parse(String line) {
        // naive implementation
        int index = line.indexOf('>') + 1;
        return new Entry(line.substring(0, index), line.substring(index));
    }

    class Entry {
        final String tag;
        final String value;

        Entry(String tag, String value) {
            this.tag = tag;
            this.value = value;
        }

        @Override
        public String toString() {
            return tag + value;
        }
    }

    class Document {
        List<Entry> entries = new ArrayList<Entry>();

        void add(Entry entry) {
            entries.add(entry);
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }

}
