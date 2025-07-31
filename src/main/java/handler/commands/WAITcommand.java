package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.SelectorRegistry;
import handler.WaitClient;
import handler.WaitClientManager;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;

public class WAITcommand implements Command {

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() != 3) {
            return "-ERR wrong number of arguments for 'wait' command\r\n";
        }

        int numReplicasToWaitFor = Integer.parseInt(args.get(1));
        long timeoutMs = Long.parseLong(args.get(2));
        long currentOffset = ServerConfig.getMaster_offset();

        // If we are waiting for 0 replicas, or there are no replicas, return immediately.
        if (numReplicasToWaitFor == 0 || ReplicaManager.getReplicas().isEmpty()) {
            return ":" + ReplicaManager.getReplicas().size() + "\r\n";
        }

        // Check if enough replicas are ALREADY synchronized.
        int acks = 0;
        for (ReplicaInfo r : ReplicaManager.getReplicas()) {
            if (r.getReplicationOffset() >= currentOffset) {
                acks++;
            }
        }

        if (acks >= numReplicasToWaitFor) {
            return ":" + acks + "\r\n";
        }

        // --- THIS IS THE CRITICAL FIX ---
        // Proactively ask all replicas for their offset right now.
        broadcastGetAck();

        // Add this client to the wait list to be checked by the background thread.
        WaitClient client = new WaitClient(clientChannel, currentOffset, numReplicasToWaitFor, timeoutMs);
        WaitClientManager.addWaitClient(client);

        // Return null to signify that the response will be sent asynchronously.
        return null;
    }

    private void broadcastGetAck() {
        List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
        if (replicas.isEmpty()) {
            return;
        }

        byte[] getAckCommand = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".getBytes();

        for (ReplicaInfo replica : replicas) {
            if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                replica.getWriteQueue().add(ByteBuffer.wrap(getAckCommand));
                SelectionKey key = replica.getChannel().keyFor(SelectorRegistry.getSelector());
                if (key != null && key.isValid()) {
                    synchronized (key) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    }
                }
            }
        }
        SelectorRegistry.getSelector().wakeup();
    }
}