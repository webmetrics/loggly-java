package biz.neustar.loggly;

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
import org.apache.http.params.HttpParams;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    private String inputUrl;
    private int maxThreads;
    private int backlog;

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
        this.maxThreads = maxThreads;
        this.backlog = backlog;

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

        HttpParams params = new BasicHttpParams();
        ConnManagerParams.setMaxTotalConnections(params, maxThreads);
        ConnPerRouteBean connPerRoute = new ConnPerRouteBean(maxThreads);
        ConnManagerParams.setMaxConnectionsPerRoute(params, connPerRoute);

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

        pool.submit(new Runnable() {
            // todo: put in better retry logic!

            @Override
            public void run() {
                try {
                    HttpPost post = new HttpPost(inputUrl);
                    StringWriter writer = new StringWriter();
                    OM.writeValue(writer, map);
                    post.setEntity(new StringEntity(writer.toString()));
                    post.setHeader("Content-Type", "application/x-www-form-urlencoded");
                    HttpResponse response = httpClient.execute(post);
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode != 200) {
                        System.err.printf("Got back %d code from Loggly\n", statusCode);
                    }
                    response.getEntity().getContent().close();
                } catch (Exception e) {
                    System.err.printf("Could not send to %s\n", inputUrl);
                    e.printStackTrace();
                }
            }
        });
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
}
