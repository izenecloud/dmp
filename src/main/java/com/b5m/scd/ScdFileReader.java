package com.b5m.scd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Read SCD files with Iterator-like interface.
 * 
 * @author Paolo D'Apice
 */
final class ScdFileReader implements Closeable {

    private final static Log log = LogFactory.getLog(ScdFileReader.class);

    final static String DOCID_TAG = "<DOCID>";

    private final BufferedReader reader;

    private Document current;
    private Document next;

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

    private boolean nextDocument() throws IOException {
        String line = null;
        while (null != (line = reader.readLine())) {
            if (StringUtils.isBlank(line)) continue;
            
            Entry e = Entry.parse(line);
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

}
