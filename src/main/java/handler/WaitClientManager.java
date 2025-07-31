package handler;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class WaitClientManager {
    private static final List<WaitClient> waitClients = new CopyOnWriteArrayList<>();

    public static void addWaitClient(WaitClient client) {
        waitClients.add(client);
    }

    public static List<WaitClient> getAll() {
        return waitClients;
    }

    public static void remove(WaitClient client) {
        waitClients.remove(client);
    }
    public static boolean isWaiting(SocketChannel channel) {
        for (WaitClient client : waitClients) {
            if (client.getChannel().equals(channel)) {
                return true;
            }
        }
        return false;
    }
}

