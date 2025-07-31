package store;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap; // <-- Import this

public class KeyValueStore {
    private static final KeyValueStore INSTANCE = new KeyValueStore();

    // Use ConcurrentHashMap for thread-safe operations
    private final Map<String, ValueWithExpiry> store = new ConcurrentHashMap<>(); // <-- The fix

    private KeyValueStore() {}

    public static KeyValueStore getInstance() {
        return INSTANCE;
    }

    public void set(String key, String value, long expiryMs) {
        System.out.println("in keyval");
        Instant setTime = Instant.now();
        store.put(key, new ValueWithExpiry(value, expiryMs, setTime));
    }

    public String get(String key) {
        ValueWithExpiry pair = store.get(key);
        if (pair == null) {
            return null;
        }

        if (pair.isExpired()) {
            // In a concurrent map, 'remove' can be called safely.
            store.remove(key, pair); // More robust concurrent removal
            return null;
        }
        return pair.getValue();
    }

    public void clear() {
        store.clear();
    }
}