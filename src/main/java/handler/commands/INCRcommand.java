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
        if(value==null){
            keyValueStore.set(key,"1", expiry);
            return ":1\r\n";
        }
        int val = 0;
        try {
            val = Integer.parseInt(value);
        }catch (NumberFormatException e){
            return "-ERR value is not an integer or out of range\r\n";
        }

        val=val+1;
        keyValueStore.set(key,Integer.toString(val), expiry);

        return ":"+val +"\r\n";
    }
}
