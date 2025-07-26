package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.util.List;

public class RpushCommand implements Command {

    KeyValueStore keyValueStore = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args) {
        if(args.size()<2) return "-ERR wrong number of arguments for 'RPUSH'\r\n";

        String key=args.get(1);
        String value= args.get(2);

        return keyValueStore.setList(key,value);
    }
}
