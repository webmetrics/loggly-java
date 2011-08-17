package biz.neustar.loggly;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple class that allows for easily sending JSON log messages to Loggly.
 */
public class Loggly {
    private static final Map<String, Object> GLOBAL = new HashMap<String, Object>();
    private static final ThreadLocal<Loggly> THREAD_LOCAL = new ThreadLocal<Loggly>() {
        @Override
        protected Loggly initialValue() {
            return new Loggly();
        }
    };
    private Map<String, Object> local = new HashMap<String, Object>(GLOBAL);

    public synchronized static void addGlobal(String key, Object o) {
        GLOBAL.put(key, o);
    }

    public static void add(String key, Object o) {
        THREAD_LOCAL.get().local.put(key, o);
    }

    public static void clear() {
        THREAD_LOCAL.get().local.clear();
    }

    static Map<String, Object> getMap() {
        return new HashMap<String, Object>(THREAD_LOCAL.get().local);
    }
}
