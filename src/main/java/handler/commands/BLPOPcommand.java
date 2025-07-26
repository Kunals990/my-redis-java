package handler.commands;

import handler.BlockingClientManager;
import handler.Command;
import store.ListStore;

import java.nio.channels.SocketChannel;
import java.util.List;

public class BLPOPcommand implements Command {

    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        if (args.size() < 3) return "-ERR wrong number of arguments for 'BLPOP'\r\n";

        String key = args.get(1);
        double timeoutSeconds = Double.parseDouble(args.get(2));
        long timeoutMillis = (long) (timeoutSeconds * 1000);

        List<String> list = listStore.getList(key);

        if (list != null && !list.isEmpty()) {
            String value = list.removeFirst();
            return "*2\r\n$" + key.length() + "\r\n" + key + "\r\n" +
                    "$" + value.length() + "\r\n" + value + "\r\n";
        }

        if (timeoutMillis <= 0) {
            // Infinite block — store with 0
            BlockingClientManager.getInstance().addBlockedClient(key, clientChannel);
        } else {
            // Timeout-based blocking
            BlockingClientManager.getInstance().addBlockedClient(key, clientChannel, timeoutMillis);
        }

        return null;
    }
}
