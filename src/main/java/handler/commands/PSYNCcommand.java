package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class PSYNCcommand implements Command {

    private static final String RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        String replicationId = ServerConfig.getMaster_replid();
        String fullResync = "+FULLRESYNC " + replicationId + " 0\r\n";

        fullyWrite(clientChannel, ByteBuffer.wrap(fullResync.getBytes()));
        sendEmptyRDB(clientChannel);

        // After handshake is sent, update the replica's state to ONLINE
        ReplicaInfo replica = ReplicaManager.getReplicaByChannel(clientChannel);
        if (replica != null) {
            replica.setState(ReplicaInfo.ReplicaState.ONLINE);
            System.out.println("Replica at port " + replica.getListeningPort() + " is now ONLINE.");
        }

        return null;
    }

    private void sendEmptyRDB(SocketChannel clientChannel) throws IOException {
        byte[] rdbBytes = hexToBytes(RDB_HEX);
        String header = "$" + rdbBytes.length + "\r\n";

        // Log and write the RDB header, ensuring it's fully sent
        System.out.println("MASTER SENDING HEADER: " + header.replace("\r\n", "\\r\\n"));
        fullyWrite(clientChannel, ByteBuffer.wrap(header.getBytes()));

        // Log and write the RDB content, ensuring it's fully sent
        System.out.println("MASTER SENDING RDB BYTES of length: " + rdbBytes.length);
        fullyWrite(clientChannel, ByteBuffer.wrap(rdbBytes));
    }

    // Helper method to ensure the entire buffer is written to the channel
    private void fullyWrite(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

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