package com.b5m.couchbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A RecordWriter that writes the reduce output to a Couchbase server.
 *
 * @author Paolo D'Apice
 */
final class CouchbaseRecordWriter extends RecordWriter<Text, Text> {

    private final static Log log = LogFactory.getLog(CouchbaseRecordWriter.class);

    private final static int BATCH_SIZE = 10000;

    private final int expiration;
    private final CouchbaseClient client;

    private final BlockingQueue<KV> queue = new LinkedBlockingQueue<KV>();

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

        this.expiration = conf.getExpiration();
        this.client = new CouchbaseClient(hosts, conf.getBucket(), conf.getPassword());
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        if (queue.size() > BATCH_SIZE) {
            drainQueue();
        }

        String k = key.toString();
        String v = value.toString();

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

    // Writes to Couchbase and keep record into the queue.
    private void enqueue(String key, String value) {
        queue.add(new KV(key, value, client.set(key, expiration, value)));
        if (log.isDebugEnabled()) log.debug("enqueued: key=" + key +" value=" + value);
    }

    // Ensures that records has been written to Couchbase.
    private void drainQueue() {
        if (log.isDebugEnabled()) log.debug("draining queue");

        Queue<KV> list = new LinkedList<KV>();
        queue.drainTo(list);
        if (log.isDebugEnabled()) log.debug("checking " + list.size() + " records");

        KV kv;
        while ((kv = list.poll()) != null) {
            try {
                if (!kv.status.get().booleanValue()) { // record not written
                    //log.warn(String.format("record with key [%s] not written, retrying", kv.key));
                    TimeUnit.MILLISECONDS.sleep(10); // XXX magic value
                    enqueue(kv.key, kv.value);
                }
            } catch (ExecutionException e) {
                //log.warn("error while draining queue: " + e.getMessage());
                retry(kv, list);
            } catch (InterruptedException e) {
                //log.warn("error while draining queue: " + e.getMessage());
                retry(kv, list);
            }
        }
    }

    // put back into the queue this kv and all the others in list
    private void retry(KV kv, Queue<KV> list) {
        enqueue(kv.key, kv.value);
        while ((kv = list.poll()) != null) {
            enqueue(kv.key, kv.value);
        }
    }

    // Key-Value pair holding values sent to Couchbase and the set operation result.
    private class KV {
        final String key;
        final String value;
        final Future<Boolean> status;

        KV(String key, String value, Future<Boolean> status) {
            this.key = key;
            this.value = value;
            this.status = status;
        }
    }

}
