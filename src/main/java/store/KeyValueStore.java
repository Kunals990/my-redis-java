package store;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {
    private static final KeyValueStore INSTANCE = new KeyValueStore();

    private final Map<String,Pair> store = new HashMap<>();

    private KeyValueStore() {}

    public static KeyValueStore getInstance() {
        return INSTANCE;
    }

    public void set(String key, String value,Integer time) {
        Instant setTime = Instant.now();
        store.put(key,new Pair(value,time,setTime));
    }

    public String get(String key) {

        Pair pair = store.get(key);
        if(pair==null) return null;

        if(pair.expiry==-1) return pair.value;

        Instant now = Instant.now();
        long elapsed = Duration.between(pair.setTime, now).toMillis();

        if (elapsed > pair.expiry) {
            store.remove(key);
            return null;
        }
        return pair.value;
    }

    public void clear() {
        store.clear();  // helpful for testing
    }
}
