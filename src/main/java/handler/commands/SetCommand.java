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

        if (args.size() < 3) return "-ERR wrong number of arguments for 'set'\r\n";

        int expiry = -1; // -1 means no expiry

        if (args.size() == 5 && args.get(3).equalsIgnoreCase("PX")) {
            try {
                expiry = Integer.parseInt(args.get(4));
            } catch (NumberFormatException e) {
                return "-ERR PX value is not a valid integer\r\n";
            }
        }

        String key1 = args.get(1);
        String value = args.get(2);

        store.set(key1,value,expiry);

        if (ServerConfig.getRole().equalsIgnoreCase("master")) {
            List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
            if (!replicas.isEmpty()) {
                byte[] commandBytes = RESPUtils.buildCommand(args);
                ServerConfig.incrementMasterOffset(commandBytes.length);

                for (ReplicaInfo replica : replicas) {
                    // ONLY propagate to replicas that have completed the handshake
                    if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                        SocketChannel replicaChannel = replica.getChannel();
                        replica.getWriteQueue().add(ByteBuffer.wrap(commandBytes));
                        SelectionKey key = replicaChannel.keyFor(selector);
                        if (key != null && key.isValid()) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                    }
                }
                // After queueing writes for all replicas, wake up the selector
                selector.wakeup();
            }
        }

        return "+OK\r\n";
    }
}
