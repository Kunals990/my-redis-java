package handler.commands;

import handler.Command;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class REPLCONFcommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.get(1).equalsIgnoreCase("capa")) {
            return "+OK\r\n";
        }

        if(args.size()==3 && args.get(1).equalsIgnoreCase("listening-port")){
            int port = Integer.parseInt(args.get(2));

            ReplicaManager.addReplica(clientChannel, port);
            return "+OK\r\n";
        }
        return "-ERR Incorrect number of arguments for REPLCONF\r\n";

    }
}
