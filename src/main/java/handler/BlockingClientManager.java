package handler;

import java.nio.channels.SocketChannel;
import java.util.*;

public class BlockingClientManager {
    private static final BlockingClientManager instance = new BlockingClientManager();

    // key -> queue of blocked clients
    private final Map<String, Queue<BlockedClient>> blockedClients = new HashMap<>();

    public static BlockingClientManager getInstance() {
        return instance;
    }

    public synchronized void addBlockedClient(String key, SocketChannel client) {
        blockedClients.computeIfAbsent(key, k -> new LinkedList<>())
                .add(new BlockedClient(client, System.currentTimeMillis(), 0)); // 0 means no timeout
    }

    public synchronized void addBlockedClient(String key, SocketChannel client, long timeoutMillis) {
        blockedClients.computeIfAbsent(key, k -> new LinkedList<>())
                .add(new BlockedClient(client, System.currentTimeMillis(), timeoutMillis));
    }

    public synchronized void addBlockedClientForStreams(List<String> keys, SocketChannel client, long timeoutMillis) {
        for (String key : keys) {
            blockedClients.computeIfAbsent(key, k -> new LinkedList<>())
                    .add(new BlockedClient(client, System.currentTimeMillis(), timeoutMillis));
        }
    }

    // ✅ This is used by RPUSH
    public synchronized SocketChannel getNextBlockedClient(String key) {
        Queue<BlockedClient> queue = blockedClients.getOrDefault(key, new LinkedList<>());
        BlockedClient bc = queue.poll();
        return bc != null ? bc.channel : null;
    }

    // ✅ This is used by timeout-checking logic
    public synchronized BlockedClient pollExpiredClient(String key) {
        Queue<BlockedClient> queue = blockedClients.get(key);
        if (queue == null) return null;

        long now = System.currentTimeMillis();
        while (!queue.isEmpty()) {
            BlockedClient bc = queue.peek();
            if (bc.timeoutMillis > 0 && now - bc.startTime >= bc.timeoutMillis) {
                return queue.poll();
            } else {
                break;
            }
        }
        return null;
    }

    public synchronized List<BlockedClient> getExpiredClients() {
        List<BlockedClient> expired = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Queue<BlockedClient> queue : blockedClients.values()) {
            while (!queue.isEmpty()) {
                BlockedClient c = queue.peek();
                if (c.timeoutMillis > 0 && now - c.startTime >= c.timeoutMillis) {
                    expired.add(queue.poll());
                } else {
                    break; // queue is time-ordered
                }
            }
        }
        return expired;
    }
}
