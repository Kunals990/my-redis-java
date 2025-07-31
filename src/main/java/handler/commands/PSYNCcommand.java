package handler.commands;

import config.ServerConfig;
import handler.Command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class PSYNCcommand implements Command {

    private static final String RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        String replicationId = ServerConfig.getMaster_replid();
        String fullResync = "+FULLRESYNC "+replicationId+" 0\r\n";

        clientChannel.write(ByteBuffer.wrap(fullResync.getBytes()));

        sendEmptyRDB(clientChannel);

        return null;
    }

    private void sendEmptyRDB(SocketChannel clientChannel) throws IOException {
        byte[] rdbBytes = hexToBytes(RDB_HEX);

        String header = "$" + rdbBytes.length + "\r\n";
        clientChannel.write(ByteBuffer.wrap(header.getBytes()));

        clientChannel.write(ByteBuffer.wrap(rdbBytes));
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte)
                    ((Character.digit(hex.charAt(i), 16) << 4)
                            + Character.digit(hex.charAt(i + 1), 16));
        }

        return data;
    }


}
