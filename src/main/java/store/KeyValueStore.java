package store;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {
    private static final KeyValueStore INSTANCE = new KeyValueStore();
//    private final Map<String, String> store = new HashMap<>();

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
       Integer expiry = store.get(key).expiry;
       Instant setTime = store.get(key).setTime;
       Instant currTime = Instant.now();

       long timeDiff = Duration.between(currTime,setTime).toMillis();
       if(timeDiff>expiry){
           return null;
       }
        return store.get(key).value;
    }

    public void clear() {
        store.clear();  // helpful for testing
    }
}
