package store;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStore {

    private static final StreamStore INSTANCE = new StreamStore();

    private final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();

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

    public StreamEntry getLastEntry(String streamKey) {
        List<StreamEntry> entries = streams.get(streamKey);
        if (entries == null || entries.isEmpty()) return null;
        return entries.getLast();
    }

    public List<String> readRangeAfter(String streamKey, String minId) {
        List<StreamEntry> entries = streams.get(streamKey);
        List<String> results = new ArrayList<>();

        if (entries == null) return results;

        for (StreamEntry entry : entries) {
            if (entry.getId().compareTo(minId) > 0) {
                results.add(entry.getId());
                List<String> flatFields = new ArrayList<>();
                for (Map.Entry<String, String> field : entry.getFields().entrySet()) {
                    flatFields.add(field.getKey());
                    flatFields.add(field.getValue());
                }
                results.addAll(flatFields);
            }
        }

        return results;
    }



}