package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.SelectorRegistry;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;
import store.KeyValueStore;
import util.RESPUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SetCommand implements Command {

    // This command handler is now completely stateless.
    // It has no member variables.

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        // --- 1. Argument Parsing ---
        if (args.size() < 3) {
            return "-ERR wrong number of arguments for 'set'\r\n";
        }
        final String key = args.get(1);
        final String value = args.get(2);
        long px = -1; // Expiry time in milliseconds

        if (args.size() > 3) {
            for (int i = 3; i < args.size(); i++) {
                if ("px".equalsIgnoreCase(args.get(i)) && i + 1 < args.size()) {
                    px = Long.parseLong(args.get(i + 1));
                    i++; // Skip the value
                }
            }
        }

        // --- 2. Data Storage ---
        // This is executed by both Master and Replica
        KeyValueStore.getInstance().set(key, value, px);

        // --- 3. Role-Specific Logic ---
        if (ServerConfig.isMaster()) {
            // If we are the master, propagate the command to all online replicas.
            propagateToReplicas(args);
            // And send OK back to the original client.
            return "+OK\r\n";
        } else {
            // If we are a replica, we have stored the data. We are done.
            // DO NOT send a response back to the master.
            return null;
        }
    }

    private void propagateToReplicas(List<String> args) {
        List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
        if (replicas.isEmpty()) {
            return;
        }

        // Build the command once to send to all replicas
        byte[] commandBytes = RESPUtils.buildCommand(args);

        for (ReplicaInfo replica : replicas) {
            if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                // Add the command to the replica's non-blocking write queue
                replica.getWriteQueue().add(ByteBuffer.wrap(commandBytes));

                // Signal the main selector that this replica's channel has data to write
                SocketChannel replicaChannel = replica.getChannel();
                SelectionKey key = replicaChannel.keyFor(SelectorRegistry.getSelector());
                if (key != null && key.isValid()) {
                    // Use a synchronized block to safely modify interestOps from a different thread
                    synchronized (key) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    }
                }
            }
        }
        // After queueing all writes, wake up the main selector to process them
        SelectorRegistry.getSelector().wakeup();
    }
}