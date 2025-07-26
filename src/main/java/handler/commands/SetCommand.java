package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.util.List;

public class SetCommand implements Command {

    KeyValueStore store = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args) {

        if (args.size() < 3) return "-ERR wrong number of arguments for 'set'\r\n";

        String key = args.get(1);
        String value = args.get(2);

        store.set(key,value);
        return "+OK\r\n";
    }
}
