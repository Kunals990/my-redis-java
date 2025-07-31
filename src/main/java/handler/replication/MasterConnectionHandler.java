package handler.replication;

import handler.Command;
import handler.CommandRegistry;
import store.KeyValueStore;
import util.RESPUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

// This class manages the replica's connection TO the master
public class MasterConnectionHandler {
    private final String masterHost;
    private final int masterPort;
    private final int myPort;
    private final SocketChannel masterChannel;
    private final Selector selector;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
    private long replicationOffset = 0;

    private enum HandshakeState { PING, REPLCONF_PORT, REPLCONF_CAPA, PSYNC, ONLINE }
    private HandshakeState handshakeState = HandshakeState.PING;

    public MasterConnectionHandler(String masterHost, int masterPort, int myPort, Selector selector) throws IOException {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.myPort = myPort;
        this.selector = selector;

        this.masterChannel = SocketChannel.open();
        this.masterChannel.configureBlocking(false);
        this.masterChannel.register(selector, SelectionKey.OP_CONNECT);
        this.masterChannel.connect(new InetSocketAddress(masterHost, masterPort));
    }

    public void handleConnect(SelectionKey key) throws IOException {
        if (masterChannel.isConnectionPending()) {
            masterChannel.finishConnect();
        }
        key.interestOps(SelectionKey.OP_WRITE); // Ready to send PING
        System.out.println("Replica connected to master. Starting handshake.");
        handleWrite(key); // Immediately try to write
    }

    public void handleRead(SelectionKey key) throws IOException {
        int bytesRead = masterChannel.read(readBuffer);
        if (bytesRead == -1) {
            System.out.println("Master closed connection.");
            key.cancel();
            masterChannel.close();
            return;
        }

        readBuffer.flip();
        // The parser logic is now part of this handler
        protocols.RESPParser parser = new protocols.RESPParser(readBuffer);
        while (readBuffer.hasRemaining()) {
            Object parsedObject = parser.parse();
            if (parsedObject == null) {
                break; // Incomplete command
            }
            processMasterResponse(parsedObject);
        }
        readBuffer.compact();
    }

    public void handleWrite(SelectionKey key) throws IOException {
        writeBuffer.flip();
        masterChannel.write(writeBuffer);
        writeBuffer.compact();
        key.interestOps(SelectionKey.OP_READ);
    }

    private void queueForWrite(byte[] data) {
        writeBuffer.put(data);
    }

    private void processMasterResponse(Object response) {
        switch (handshakeState) {
            case PING: // Received PONG
                System.out.println("Handshake: Received PONG.");
                queueForWrite(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
                handshakeState = HandshakeState.REPLCONF_PORT;
                masterChannel.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
                break;
            case REPLCONF_PORT: // Received OK
                System.out.println("Handshake: Received OK for REPLCONF port.");
                queueForWrite(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
                handshakeState = HandshakeState.REPLCONF_CAPA;
                masterChannel.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
                break;
            case REPLCONF_CAPA: // Received OK
                System.out.println("Handshake: Received OK for REPLCONF capa.");
                queueForWrite(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
                handshakeState = HandshakeState.PSYNC;
                masterChannel.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
                break;
            case PSYNC: // Received FULLRESYNC or RDB
                if (response instanceof String && ((String) response).toUpperCase().startsWith("FULLRESYNC")) {
                    System.out.println("Handshake: Received FULLRESYNC.");
                    // The next response will be the RDB file, which we will just consume
                } else if (response instanceof byte[]) {
                    System.out.println("Handshake: Received RDB file, now online.");
                    // After RDB, we are officially online and processing commands
                    handshakeState = HandshakeState.ONLINE;
                }
                break;
            case ONLINE:
                if (response instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> args = (List<String>) response;
                    String commandName = args.get(0).toUpperCase();

                    replicationOffset += calculateCommandSize(args); // A simple estimation

                    if ("REPLCONF".equals(commandName) && args.size() > 1 && "GETACK".equalsIgnoreCase(args.get(1))) {
                        List<String> ackCommand = List.of("REPLCONF", "ACK", String.valueOf(this.replicationOffset));
                        queueForWrite(RESPUtils.buildCommand(ackCommand));
                        masterChannel.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
                    } else {
                        Command cmdImpl = CommandRegistry.getCommand(commandName);
                        if (cmdImpl != null) {
                            try {
                                cmdImpl.execute(args, null);
                            } catch (IOException e) { /* Should not happen */ }
                        }
                    }
                }
                break;
        }
    }

    private long calculateCommandSize(List<String> commandParts) {
        long size = 0;
        // This is a rough estimation; a more accurate parser would track bytes consumed.
        for (String part : commandParts) {
            size += part.getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }
}