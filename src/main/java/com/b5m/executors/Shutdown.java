package com.b5m.executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods to be used with java.util.concurrent classes.
 *
 * @author Paolo D'Apice
 */
public class Shutdown {

    private final static Log log = LogFactory.getLog(Shutdown.class);

    public static void andWait(ExecutorService pool, long timeout, TimeUnit unit) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeout, unit)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(timeout, unit))
                    log.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

}
