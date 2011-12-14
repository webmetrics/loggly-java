package biz.neustar.loggly;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * A handler for JDK logging that sends JSON-formatted messages to Loggly.
 */
public class LogglyHandler extends Handler {
    private static final ObjectMapper OM = new ObjectMapper();

    private DefaultHttpClient httpClient;
    private ThreadPoolExecutor pool;
    private Queue<LogglySample> retryQueue;
    private boolean allowRetry = true;
    private String inputUrl;

    /**
     * Creates a handler with the specified input URL, 10 threads, and support for 5000 messages in the backlog.
     *
     * @param inputUrl - the URL provided by Loggly for sending log messages to
     * @see #LogglyHandler(String, int, int)
     */
    public LogglyHandler(String inputUrl) {
        this(inputUrl, 10, 5000);
    }

    /**
     * Creates a handler with the specified input URL, max thread count, and message backlog support.
     *
     * @param inputUrl - the URL provided by Loggly for sending log messages to
     * @param maxThreads - the max number of concurrent background threads that are allowed to send data to Loggly
     * @param backlog - the max number of log messages that can be queued up (anything beyond will be thrown away)
     */
    public LogglyHandler(String inputUrl, int maxThreads, int backlog) {
        this.inputUrl = inputUrl;

        pool = new ThreadPoolExecutor(maxThreads, maxThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(backlog),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "Loggly Thread");
                        thread.setDaemon(true);
                        return thread;
                    }
                }, new ThreadPoolExecutor.DiscardOldestPolicy());
        pool.allowCoreThreadTimeOut(true);

        retryQueue = new LinkedBlockingQueue<LogglySample>(backlog);

        Thread retryThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (allowRetry) {
                    // drain the retry requests
                    LogglySample sample = null;
                    while ((sample = retryQueue.poll()) != null) {
                        if (sample.retryCount > 10) {
                            // todo: capture statistics about the failure (exception and/or status code)
                            //       and then report on it in some sort of thoughtful way to standard err
                        } else {
                            pool.submit(sample);
                        }
                    }

                    // retry every 10 seconds
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        System.err.println("Retry sleep was interrupted, giving up on retry thread");
                        return;
                    }
                }
            }
        }, "Loggly Retry Thread");
        retryThread.setDaemon(true);
        retryThread.start();

        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, maxThreads);
        ConnPerRouteBean connPerRoute = new ConnPerRouteBean(maxThreads);
        ConnManagerParams.setMaxConnectionsPerRoute(params, connPerRoute);

        // set 15 second timeouts, since Loggly should return quickly
        params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 15000);
        params.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 15000);

        SchemeRegistry registry = new SchemeRegistry();
        try {
            registry.register(new Scheme("https", SSLSocketFactory.getSocketFactory(), 443));
        } catch (Exception e) {
            throw new RuntimeException("Could not register SSL socket factor for Loggly", e);
        }

        ThreadSafeClientConnManager connManager = new ThreadSafeClientConnManager(params, registry);


        httpClient = new DefaultHttpClient(connManager, params);

        // because the threads a daemon threads, we want to give them a chance
        // to finish up before we totally shut down
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));
    }

    @Override
    public void publish(LogRecord record) {
        final Map<String, Object> map = Loggly.getMap();

        String level = "UNKN";
        if (record.getLevel().equals(Level.WARNING)) {
            level = "WARN";
        } else if (record.getLevel().equals(Level.SEVERE)) {
            level = "SEVR";
        } else if (record.getLevel().equals(Level.INFO)) {
            level = "INFO";
        } else if (record.getLevel().equals(Level.FINE)) {
            level = "FINE";
        } else if (record.getLevel().equals(Level.FINEST)) {
            level = "FNST";
        } else if (record.getLevel().equals(Level.FINER)) {
            level = "FINR";
        } else if (record.getLevel().equals(Level.CONFIG)) {
            level = "CONF";
        } else if (record.getLevel().equals(Level.OFF)) {
            level = "OFF ";
        } else if (record.getLevel().equals(Level.ALL)) {
            level = "ALL ";
        }

        // and the log message itself
        if (record.getParameters() != null && record.getParameters().length > 0) {
            java.util.Formatter formatter = new java.util.Formatter();
            formatter.format(record.getMessage(), record.getParameters());
            map.put("message", formatter.toString());
        } else {
            map.put("message", record.getMessage());
        }

        // finally, other metadata
        map.put("thread", Thread.currentThread().getName());
        map.put("loggerName", record.getLoggerName());
        map.put("level", level);

        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            record.getThrown().printStackTrace(new PrintWriter(sw));
            map.put("stackTrace", sw.toString());
        }

        pool.submit(new LogglySample(map));
    }

    @Override
    public void flush() {
    }

    @Override
    public synchronized void close() throws SecurityException {
        if (pool.isShutdown()) {
            return;
        }

        try {
            // first, anything in the retry queue should be tried one last time and then we give up on it
            allowRetry = false;
            for (LogglySample sample : retryQueue) {
                pool.submit(sample);
            }
            retryQueue.clear();

            System.out.println("Shutting down Loggly handler - waiting 90 seconds for " + pool.getQueue().size() + " logs to finish");
            pool.shutdown();
            try {
                boolean result = pool.awaitTermination(90, TimeUnit.SECONDS);
                if (!result) {
                    System.out.println("Not all Loggly messages sent out - still had " + pool.getQueue().size() + " left :(");
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                // ignore
            }
        } finally {
            httpClient.getConnectionManager().shutdown();
            System.out.println("Loggly handler shut down");
        }
    }

    private class LogglySample implements Runnable {
        private final Map<String, Object> map;
        private int retryCount = 0;
        private Exception exception;
        private int statusCode;

        public LogglySample(Map<String, Object> map) {
            this(map, 0);
        }

        private LogglySample(Map<String, Object> map, int retryCount) {
            this.map = map;
            this.retryCount = retryCount;
        }

        @Override
        public void run() {
            HttpEntity entity = null;
            try {
                HttpPost post = new HttpPost(inputUrl);
                StringWriter writer = new StringWriter();
                OM.writeValue(writer, map);
                post.setEntity(new StringEntity(writer.toString()));
                post.setHeader("Content-Type", "application/x-www-form-urlencoded");
                HttpResponse response = httpClient.execute(post);
                entity = response.getEntity();
                statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    if (allowRetry) {
                        retryCount++;
                        retryQueue.offer(this);
                    }
                }
            } catch (Exception e) {
                if (allowRetry) {
                    exception = e;
                    retryCount++;
                    retryQueue.offer(this);
                }
            } finally {
                if (entity != null) {
                    try {
                        entity.consumeContent();
                    } catch (IOException e) {
                        // safe to ignore
                    }
                }
            }
        }
    }
}
