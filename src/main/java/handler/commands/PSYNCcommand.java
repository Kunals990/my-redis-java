package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.SelectorRegistry;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;

public class PSYNCcommand implements Command {

    // RDB file content remains the same
    private static final String RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        ReplicaInfo replica = ReplicaManager.getReplicaByChannel(clientChannel);
        if (replica == null) {
            // Should not happen if REPLCONF was handled correctly
            return null;
        }

        // 1. Prepare the FULLRESYNC response and RDB file content
        String fullResync = "+FULLRESYNC " + ServerConfig.getMaster_replid() + " 0\r\n";
        byte[] rdbBytes = hexToBytes(RDB_HEX);
        String rdbHeader = "$" + rdbBytes.length + "\r\n";

        // 2. Add all data to the replica's non-blocking write queue
        replica.getWriteQueue().add(ByteBuffer.wrap(fullResync.getBytes()));
        replica.getWriteQueue().add(ByteBuffer.wrap(rdbHeader.getBytes()));
        replica.getWriteQueue().add(ByteBuffer.wrap(rdbBytes));

        // Log what you are queueing
        System.out.println("MASTER QUEUEING FULLRESYNC for replica on port: " + replica.getListeningPort());
        System.out.println("MASTER QUEUEING RDB of length " + rdbBytes.length + " for replica on port: " + replica.getListeningPort());


        // 3. Register interest in writing to the channel
        SelectionKey key = clientChannel.keyFor(SelectorRegistry.getSelector());
        if (key != null) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }

        // 4. Update the replica's state. The data will be sent by the event loop.
        replica.setState(ReplicaInfo.ReplicaState.ONLINE);
        System.out.println("Replica at port " + replica.getListeningPort() + " is now ONLINE.");

        // No direct response is returned; the event loop handles writing
        return null;
    }

    // REMOVE the blocking fullyWrite method. It should not be used.

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}