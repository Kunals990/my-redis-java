package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.util.List;

public class GetCommand implements Command {

    KeyValueStore store = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args) {
        if (args.size() < 2) return "-ERR wrong number of arguments for 'get'\r\n";

        String key=args.get(1);
        String value = store.get(key);

        if (value == null) return "$-1\r\n";

        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
}
