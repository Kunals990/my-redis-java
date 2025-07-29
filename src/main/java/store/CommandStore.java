package store;

import java.nio.channels.SocketChannel;
import java.util.*;

public class CommandStore {
    private static final CommandStore INSTANCE = new CommandStore();

    private CommandStore() {};

    public static CommandStore getInstance(){
        return INSTANCE;
    }

    private final Map<SocketChannel, Queue<List<String>>> multiQueues = new HashMap<>();

    public String addToQueue(SocketChannel channel, List<String> args) {
        multiQueues.putIfAbsent(channel, new LinkedList<>());
        multiQueues.get(channel).add(args);
        return "+QUEUED\r\n";
    }

    public Queue<List<String>> getQueue(SocketChannel channel) {
        return multiQueues.getOrDefault(channel, new LinkedList<>());
    }

    public void clearQueue(SocketChannel channel) {
        multiQueues.remove(channel);
    }
}
