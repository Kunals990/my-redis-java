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
import java.nio.channels.SocketChannel;
import java.util.List;

public class SetCommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        if (args.size() < 3) {
            return "-ERR wrong number of arguments for 'set'\r\n";
        }
        final String key = args.get(1);
        final String value = args.get(2);
        long px = -1;

        if (args.size() > 3) {
            for (int i = 3; i < args.size(); i++) {
                if ("px".equalsIgnoreCase(args.get(i)) && i + 1 < args.size()) {
                    px = Long.parseLong(args.get(i + 1));
                    i++;
                }
            }
        }
        KeyValueStore.getInstance().set(key, value, px);

        if (ServerConfig.isMaster()) {
            propagateToReplicas(args);
            return "+OK\r\n";
        } else {
            return null;
        }
    }

    private void propagateToReplicas(List<String> args) {
        List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
        if (replicas.isEmpty()) {
            return;
        }
        byte[] commandBytes = RESPUtils.buildCommand(args);
        ServerConfig.incrementMasterOffset(commandBytes.length);

        for (ReplicaInfo replica : replicas) {
            if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                replica.getWriteQueue().add(ByteBuffer.wrap(commandBytes));
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