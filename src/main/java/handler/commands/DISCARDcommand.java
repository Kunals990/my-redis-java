package handler.commands;

import handler.Command;
import store.CommandStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class DISCARDcommand implements Command {

    CommandStore commandStore = CommandStore.getInstance();
    MULTIcommand multIcommand = MULTIcommand.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(!MULTIcommand.getInstance().isMulti(clientChannel)){
            return "-ERR DISCARD without MULTI\r\n";
        }
        else{
            multIcommand.disableMulti(clientChannel);
            commandStore.clearQueue(clientChannel);
        }
        return "+OK\r\n";
    }
}
