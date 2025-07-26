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
        String timeoutStr = args.get(2);
        int timeout = Integer.parseInt(timeoutStr);

        List<String> list = listStore.getList(key);

        if (list != null && !list.isEmpty()) {
            String value = list.removeFirst();
            return "*2\r\n$" + key.length() + "\r\n" + key + "\r\n" +
                    "$" + value.length() + "\r\n" + value + "\r\n";
        }

        BlockingClientManager.getInstance().addBlockedClient(key, clientChannel);
        return null;
    }
}
