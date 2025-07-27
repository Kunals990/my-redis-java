package handler.commands;

import handler.Command;
import store.KeyValueStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class TYPEcommand implements Command {

    KeyValueStore store = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(args.size()<2) return "-ERR wrong number of arguments for 'TYPE'\r\n";

        String key = args.get(1);
        String value = store.get(key);

        if(value==null) return "+none\r\n";

        return "+"+value.getClass().getSimpleName().toLowerCase()+"\r\n";
    }
}
