package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.nio.channels.SocketChannel;
import java.util.List;

public class SetCommand implements Command {

    KeyValueStore store = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {

        if (args.size() < 3) return "-ERR wrong number of arguments for 'set'\r\n";

        int expiry = -1; // -1 means no expiry

        if (args.size() == 5 && args.get(3).equalsIgnoreCase("PX")) {
            try {
                expiry = Integer.parseInt(args.get(4));
            } catch (NumberFormatException e) {
                return "-ERR PX value is not a valid integer\r\n";
            }
        }

        String key = args.get(1);
        String value = args.get(2);

        store.set(key,value,expiry);
        return "+OK\r\n";
    }
}
