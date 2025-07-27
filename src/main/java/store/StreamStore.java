package store;

import java.util.*;

public class StreamStore {

    private static final StreamStore INSTANCE = new StreamStore();

    private final Map<String, List<StreamEntry>> streams = new HashMap<>();

    private StreamStore() {}

    public static StreamStore getInstance() {
        return INSTANCE;
    }

    public void addEntry(String streamKey, String id, Map<String, String> fields) {
        streams.computeIfAbsent(streamKey, k -> new ArrayList<>())
                .add(new StreamEntry(id, fields));
    }

    public List<StreamEntry> getStream(String key) {
        return streams.getOrDefault(key, Collections.emptyList());
    }

    public boolean exists(String key) {
        return streams.containsKey(key);
    }
}