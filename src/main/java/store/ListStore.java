package store;

import java.util.*;

public class ListStore {

    private static final ListStore INSTANCE = new ListStore();

    private final Map<String, List<String>> listStore = new HashMap<>();

    private ListStore() {}

    public static ListStore getInstance() {
        return INSTANCE;
    }

    public String appendToList(String key, List<String> values) {
        List<String> list = listStore.computeIfAbsent(key, k -> new ArrayList<>());
        list.addAll(values);
        return ":" + list.size() + "\r\n";
    }

    public String appendToListFront(String key,List<String>values){
        List<String> list = listStore.computeIfAbsent(key, k -> new ArrayList<>());
        for (int i = values.size() - 1; i >= 0; i--) {
            list.add(0, values.get(i));
        }
        return ":" + list.size() + "\r\n";
    }

    public List<String> getList(String key) {
        return listStore.getOrDefault(key, Collections.emptyList());
    }

    public void clear() {
        listStore.clear();
    }

}
