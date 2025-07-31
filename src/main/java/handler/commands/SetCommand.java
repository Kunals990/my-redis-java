package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.SelectorRegistry;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;
import protocols.RESPBuilder;
import store.KeyValueStore;
import util.RESPUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SetCommand implements Command {

    KeyValueStore store = KeyValueStore.getInstance();

    Selector selector = SelectorRegistry.getSelector();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        // ... (your existing parsing for key, value, and PX is fine)
        if (args.size() < 3) return "-ERR wrong number of arguments for 'set'\r\n";
        String key1 = args.get(1);
        String value = args.get(2);
        // ...

        store.set(key1, value, -1); // Assuming you handle expiry separately

        if (ServerConfig.isMaster()) {
            List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
            if (!replicas.isEmpty()) {
                byte[] commandBytes = RESPUtils.buildCommand(args);
                // Increment offset ONCE
                ServerConfig.incrementMasterOffset(commandBytes.length);

                for (ReplicaInfo replica : replicas) {
                    // ONLY propagate to replicas that have completed the handshake
                    if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                        SocketChannel replicaChannel = replica.getChannel();
                        // Add the command to the queue
                        replica.getWriteQueue().add(ByteBuffer.wrap(commandBytes));
                        // Signal the main selector to handle the write
                        SelectionKey key = replicaChannel.keyFor(SelectorRegistry.getSelector());
                        if (key != null && key.isValid()) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                    }
                }
                // After queueing all writes, wake up the selector
                SelectorRegistry.getSelector().wakeup();
            }
        }

        return "+OK\r\n";
    }
}
