package handler.commands;

import handler.Command;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class REPLCONFcommand implements Command {

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        // Handle 'listening-port' and 'capa'
        if (args.get(1).equalsIgnoreCase("listening-port")) {
            if (args.size() == 3) {
                int port = Integer.parseInt(args.get(2));
                ReplicaManager.addReplica(clientChannel, port);
                return "+OK\r\n";
            }
        }

        if (args.get(1).equalsIgnoreCase("capa")) {
            return "+OK\r\n";
        }
        if (args.get(1).equalsIgnoreCase("ACK")) {
            // You will need to implement logic here later to track the replica's offset
            // For now, just returning null is fine, as no response is sent.
            return null;
        }

        return "-ERR Incorrect arguments for REPLCONF\r\n";

    }
}
