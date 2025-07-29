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
        String info="role:"+role+"\r\n";
        String master_replid=ServerConfig.getMaster_replid();
        String master_repl_offset=ServerConfig.getMaster_repl_offset();

        info+="master_replid:"+master_replid+"\n";
        info+="master_repl_offset:"+master_repl_offset+"\n";

        return "$"+info.length()+"\r\n"+info+"\r\n";
    }
}
