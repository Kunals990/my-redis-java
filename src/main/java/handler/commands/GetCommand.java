package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.nio.channels.SocketChannel;
import java.util.List;

public class GetCommand implements Command {

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        if (args.size() < 2) return "-ERR wrong number of arguments for 'get'\r\n";
        System.out.println("In get");
        String key=args.get(1);
        String value = KeyValueStore.getInstance().get(key);
        System.out.println("in get :val "+value);
        if (value == null) return "$-1\r\n";

        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
}
