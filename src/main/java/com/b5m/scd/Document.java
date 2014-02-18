package com.b5m.scd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SCD document container.
 * 
 * @author Paolo D'Apice
 */
public final class Document {

    protected List<Entry> entries = new ArrayList<Entry>();

    public void add(Entry entry) {
        entries.add(entry);
    }

    public List<Entry> entries() {
        return Collections.unmodifiableList(entries);
    }

    @Override
    public String toString() {
        return entries.toString();
    }
    
}
