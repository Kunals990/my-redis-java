package handler.commands;

import handler.Command;
import store.ListStore;

import java.nio.channels.SocketChannel;
import java.util.List;

public class LPUSHcommand implements Command {

    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String>args, SocketChannel clientChannel){
        if(args.size()<3) return "-ERR wrong number of arguments for 'RPUSH'\r\n";

        String key=args.get(1);

        List<String> values = args.subList(2, args.size());

        return listStore.appendToListFront(key,values);
    }
}
