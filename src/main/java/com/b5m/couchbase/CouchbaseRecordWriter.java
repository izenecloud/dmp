package com.b5m.couchbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A RecordWriter that writes the reduce output to a Couchbase server.
 *
 * Records are stored into a memory queue and then bulk written
 * to Couchbase when the queue reach a specified batch size.
 *
 * @author Paolo D'Apice
 */
final class CouchbaseRecordWriter<K extends Text, V> extends RecordWriter<K, V> {

    private final int batchSize;
    private final CouchbaseClient client;

    private final BlockingQueue<KV> queue = new LinkedBlockingQueue<KV>();
    private final Gson gson = new Gson();

    CouchbaseRecordWriter(CouchbaseConfiguration conf)
    throws IOException {
        List<URI> hosts = new ArrayList<URI>();
        try {
            for (String uri : conf.getUris()) {
                hosts.add(new URI(uri));
            }
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }

        this.batchSize = conf.getBatchSize();
        this.client = new CouchbaseClient(hosts, conf.getBucket(), conf.getPassword());
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
        if (queue.size() > batchSize) {
            drainQueue();
        }

        String k = key.toString();
        String v = gson.toJson(new Document(k, value));

        enqueue(k, v);
    }

    @Override
    public void close(TaskAttemptContext context)
    throws IOException, InterruptedException {
        while (!queue.isEmpty()) {
            drainQueue();
        }

        client.shutdown();
    }

    // Adds key-value record to the queue.
    private void enqueue(String k, String v) {
        queue.add(new KV(k, v, client.set(k, 0, v)));
    }

    // Writes queued records to Couchbase.
    private void drainQueue() {
        Queue<KV> list = new LinkedList<KV>();
        queue.drainTo(list);

        KV kv;
        while ((kv = list.poll()) != null) {
            try {
                if (!kv.status.get().booleanValue()) { // record not inserted
                    TimeUnit.MILLISECONDS.sleep(10); // XXX magic value
                    enqueue(kv.key, kv.value);
                }
            } catch (Exception e) {
                // put back into the queue this kv and all the others
                enqueue(kv.key, kv.value);
                while ((kv = list.poll()) != null) {
                    enqueue(kv.key, kv.value);
                }
            }
        }
    }
}

/*
 * Key-Value pair holding values sent to Couchbase and the set operation result.
 */
final class KV {
    final String key;
    final String value;
    final Future<Boolean> status;

    KV(String key, String value, Future<Boolean> status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }
}

/* Document to be written to Couchbase */
final class Document {
    final String key;
    final Object value;
    Document(String key, Object value) {
        this.key = key;
        this.value = value;
    }
}

