package com.b5m.couchbase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
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
    private final int expiration;

    public CouchbaseConfiguration(String uris, String bucket, String password, String expiration) {
        this.uris = Arrays.asList(uris.split(","));
        this.bucket = bucket;
        this.password = password;
        this.expiration = Integer.parseInt(expiration);
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

    public int getExpiration() {
        return expiration;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("couchbase://").append(bucket).append("@");
        for (Iterator<String> it = uris.iterator(); it.hasNext();) {
            String string = it.next();
            try {
                sb.append(new URI(string).getHost());
            } catch (URISyntaxException e) {
                sb.append(string);
            }
            if (it.hasNext()) sb.append(",");
        }
        return sb.toString();
    }

}


