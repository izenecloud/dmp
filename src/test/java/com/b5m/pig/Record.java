package com.b5m.pig;

import com.b5m.utils.Tuples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * JavaBean for a single DMP record (for tests only).
 *
 * @author Paolo D'Apice
 */
public final class Record {

    private final static Log log = LogFactory.getLog(Record.class);

    private String uuid;
    private Map<String, Integer> categories = new HashMap<String, Integer>();

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Map<String, Integer> getCategories() {
        return Collections.unmodifiableMap(categories);
    }

    public void setCategories(Map<String, Integer> categories) {
        this.categories.clear();
        this.categories.putAll(categories);
    }

    public static Record fromJson(String json) throws IOException {
        log.debug("json: " + json);

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Record.class);
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof Record)) return false;
        Record r = (Record) o;

        return uuid.equals(r.uuid)
            && categories.equals(r.categories);
    }

    @SuppressWarnings("unchecked")
    public static Record fromPig(String line) throws Exception {
        log.debug("pig: " + line);

        Tuple t = Tuples.fromString(line, "(chararray, [int])");
        Record r = new Record();
        r.setUuid((String) t.get(0));
        r.setCategories((Map<String, Integer>) t.get(1));
        return r;
    }
}

