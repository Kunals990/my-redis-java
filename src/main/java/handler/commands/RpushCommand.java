package handler.commands;

import handler.Command;
import store.KeyValueStore;
import store.ListStore;

import java.util.List;

public class RpushCommand implements Command {

    KeyValueStore keyValueStore = KeyValueStore.getInstance();
    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String> args) {
        if(args.size()<3) return "-ERR wrong number of arguments for 'RPUSH'\r\n";

        String key=args.get(1);

        List<String> values = args.subList(2, args.size());
        return listStore.appendToList(key,values);
    }
}
