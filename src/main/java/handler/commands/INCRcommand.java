package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.List;

public class INCRcommand implements Command {

    KeyValueStore keyValueStore = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() < 2) return "-ERR wrong number of arguments for 'INCR'\r\n";
        String key=args.get(1);
        int expiry=-1;
        String value= keyValueStore.get(key);
        int val = Integer.parseInt(value);
        val=val+1;
        keyValueStore.set(key,Integer.toString(val), expiry);

        return ":"+val +"\r\n";
    }
}
