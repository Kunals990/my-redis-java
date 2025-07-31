package handler.commands;

import handler.Command;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class REPLCONFcommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() < 2) {
            return "-ERR wrong number of arguments for 'replconf'\r\n";
        }
        String subCommand = args.get(1).toUpperCase();

        switch (subCommand) {
            case "LISTENING-PORT":
                if (args.size() == 3) {
                    int port = Integer.parseInt(args.get(2));
                    ReplicaManager.addReplica(clientChannel, port);
                    return "+OK\r\n";
                }
                break;
            case "CAPA":
                return "+OK\r\n";
            case "ACK":
                if (args.size() == 3) {
                    ReplicaInfo replica = ReplicaManager.getReplicaByChannel(clientChannel);
                    if (replica != null) {
                        long offset = Long.parseLong(args.get(2));
                        replica.setReplicationOffset(offset);
                    }
                    return null;
                }
                break;
        }
        return "-ERR Unrecognized REPLCONF subcommand\r\n";
    }
}