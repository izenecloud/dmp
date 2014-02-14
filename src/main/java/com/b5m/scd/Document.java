package com.b5m.scd;

import java.util.ArrayList;
import java.util.List;

/**
 * SCD document container.
 * 
 * @author Paolo D'Apice
 */
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
