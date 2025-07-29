package handler.commands;

import handler.Command;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class INFOcommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {

        if(!args.get(1).equals("replication")){
            return "-ERR Invalid number of arguments";
        }
        String info="role:master";
        return "$"+info.length()+"\r\n"+info+"\r\n";
    }
}
