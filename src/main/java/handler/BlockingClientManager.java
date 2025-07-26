package handler;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class BlockingClientManager {
    private static final BlockingClientManager instance = new BlockingClientManager();

    // key -> queue of blocked clients
    private final Map<String, Queue<SocketChannel>> blockedClients = new HashMap<>();

    public static BlockingClientManager getInstance() {
        return instance;
    }

    public synchronized void addBlockedClient(String key, SocketChannel client) {
        blockedClients.computeIfAbsent(key, k -> new LinkedList<>()).add(client);
    }

    public synchronized SocketChannel getNextBlockedClient(String key) {
        Queue<SocketChannel> queue = blockedClients.get(key);
        if (queue == null || queue.isEmpty()) return null;
        return queue.poll();
    }
}
