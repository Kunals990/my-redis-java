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
        if (args.size() < 3) return "-ERR wrong number of arguments for 'INCR'\r\n";
        String key=args.get(1);
        String val=keyValueStore.get(key);
        int expiry=-1;
        if(val==null){
            keyValueStore.set(key,val,expiry);
        }
        val=val+1;
        keyValueStore.set(key,val, expiry);

        return ":"+val+"\r\n";
    }
}
