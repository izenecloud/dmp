package com.b5m.scd;

/**
 * SCD entry, consisting in tag and a value.
 *
 * @author Paolo D'Apice
 */
public final class Entry {

    protected final String tag;
    protected final String value;

    public Entry(String tag, String value) {
        this.tag = tag;
        this.value = value;
    }

    public String getTag() {
        return tag;
    }

    public String getTagName() {
        return tag.substring(1, tag.length() - 1);
    }

    public String getValue() {
        return value;
    }

    /**
     * Parse an SCD line containing a tag and a value.
     */
    public static Entry parse(String line) {
        // naive implementation
        int index = line.indexOf('>') + 1;
        return new Entry(line.substring(0, index), line.substring(index));
    }

    @Override
    public String toString() {
        return tag + value;
    }
    
}
