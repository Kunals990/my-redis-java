package store;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {
    private static final KeyValueStore INSTANCE = new KeyValueStore();

    private final Map<String, ValueWithExpiry> store = new HashMap<>();

    private KeyValueStore() {}

    public static KeyValueStore getInstance() {
        return INSTANCE;
    }

    public void set(String key, String value,Integer time) {
        Instant setTime = Instant.now();
        store.put(key,new ValueWithExpiry(value,time,setTime));
    }

    public String get(String key) {

        ValueWithExpiry pair = store.get(key);
        if(pair==null) return null;

        if(!pair.isExpiryPresent()) return pair.getValue();

        if (pair.isExpired()) {
            store.remove(key);
            return null;
        }
        return pair.getValue();
    }

    public void clear() {
        store.clear();  // helpful for testing
    }
}
