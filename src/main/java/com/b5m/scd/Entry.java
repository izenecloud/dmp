package com.b5m.scd;

/**
 * SCD entry, consisting in tag and a value.
 *
 * @author Paolo D'Apice
 */
final class Entry {

    final String tag;
    final String value;

    Entry(String tag, String value) {
        this.tag = tag;
        this.value = value;
    }

    String untag() {
        return tag.substring(1, tag.length() - 1);
    }

    static Entry parse(String line) {
        // naive implementation
        int index = line.indexOf('>') + 1;
        return new Entry(line.substring(0, index), line.substring(index));
    }

    @Override
    public String toString() {
        return tag + value;
    }
    
}
