package handler.commands;

import handler.Command;
import handler.ServerConfig;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class INFOcommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() != 2 || !args.get(1).equals("replication")) {
            return "-ERR Invalid number of arguments for 'INFO'\r\n";
        }

        String role= ServerConfig.getRole();
        String info="role:"+role;
        return "$"+info.length()+"\r\n"+info+"\r\n";
    }
}
