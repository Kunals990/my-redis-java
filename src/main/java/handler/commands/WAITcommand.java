package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.WaitClient;
import handler.WaitClientManager;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;


public class WAITcommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {

        int numReplicasToWaitFor = Integer.parseInt(args.get(1));
        long timeoutMs = Long.parseLong(args.get(2));
        long currentOffset = ServerConfig.getMaster_offset();

        int acks = 0;
        for (ReplicaInfo r : ReplicaManager.getReplicas()) {
            if (r.getReplicationOffset() >= currentOffset) {
                acks++;
            }
        }

        if (acks >= numReplicasToWaitFor) {
            return ":" + acks + "\r\n";
        }

        WaitClient client = new WaitClient(clientChannel, currentOffset, numReplicasToWaitFor, timeoutMs);
        WaitClientManager.addWaitClient(client);

        return null;
    }
}
