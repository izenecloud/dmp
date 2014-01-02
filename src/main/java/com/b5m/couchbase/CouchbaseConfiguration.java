package com.b5m.couchbase;

import java.util.Arrays;
import java.util.List;

/**
 * Container for Couchbase configuration parameters.
 *
 * @author Paolo D'Apice
 */
public class CouchbaseConfiguration {

    private final List<String> uris;
    private final String bucket;
    private final String password;
    private final int batchSize;

    public CouchbaseConfiguration(String uris, String bucket, String password, String batchSize) {
        this.uris = Arrays.asList(uris.split(","));
        this.bucket = bucket;
        this.password = password;
        this.batchSize = Integer.valueOf(batchSize);
    }

    public List<String> getUris() {
        return uris;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPassword() {
        return password;
    }

    public int getBatchSize() {
        return batchSize;
    }
}


