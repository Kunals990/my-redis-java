package handler.replication;

import handler.Command;
import handler.CommandRegistry;
import protocols.RESPParser;
import util.RESPUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

// This class manages the replica's connection TO the master, using the main event loop.
public class MasterConnectionHandler {

    private final SocketChannel masterChannel;
    private final Selector selector;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
    private long replicationOffset = 0;

    private final int myPort;
    private enum HandshakeState { PING, REPLCONF_PORT, REPLCONF_CAPA, PSYNC, ONLINE }
    private HandshakeState handshakeState = HandshakeState.PING;

    public MasterConnectionHandler(String masterHost, int masterPort, int myPort, Selector selector) throws IOException {
        this.myPort = myPort;
        this.selector = selector;

        this.masterChannel = SocketChannel.open();
        this.masterChannel.configureBlocking(false);

        // *** THE FIX IS HERE: Add 'this' as the attachment ***
        this.masterChannel.register(selector, SelectionKey.OP_CONNECT, this);

        this.masterChannel.connect(new InetSocketAddress(masterHost, masterPort));
    }

    public void handleConnect(SelectionKey key) throws IOException {
        if (masterChannel.isConnectionPending()) {
            masterChannel.finishConnect();
        }
        System.out.println("Replica connected to master. Starting handshake.");

        // Start the handshake by queueing a PING and asking to be notified when the socket is writable.
        queueForWrite(RESPUtils.buildCommand(List.of("PING")));
        key.interestOps(SelectionKey.OP_WRITE);
    }

    public void handleRead(SelectionKey key) throws IOException {
        readBuffer.clear();
        int bytesRead = masterChannel.read(readBuffer);
        if (bytesRead == -1) {
            System.out.println("Master closed connection.");
            key.cancel();
            masterChannel.close();
            return;
        }

        readBuffer.flip();
        RESPParser parser = new RESPParser(readBuffer);

        while (readBuffer.hasRemaining()) {
            Object parsedObject = parser.parse();
            if (parsedObject == null) break; // Incomplete command

            processMasterResponse(key, parsedObject);
        }
        readBuffer.compact();
    }

    public void handleWrite(SelectionKey key) throws IOException {
        writeBuffer.flip();
        while (writeBuffer.hasRemaining()) {
            masterChannel.write(writeBuffer);
        }
        writeBuffer.compact();
        // After writing, we are only interested in reading the response.
        key.interestOps(SelectionKey.OP_READ);
    }

    private void queueForWrite(byte[] data) {
        writeBuffer.put(data);
    }

    private void processMasterResponse(SelectionKey key, Object response) {
        switch (handshakeState) {
            case PING:
                System.out.println("Handshake: Received PONG.");
                queueForWrite(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
                handshakeState = HandshakeState.REPLCONF_PORT;
                key.interestOps(SelectionKey.OP_WRITE);
                break;
            case REPLCONF_PORT:
                System.out.println("Handshake: Received OK for REPLCONF port.");
                queueForWrite(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
                handshakeState = HandshakeState.REPLCONF_CAPA;
                key.interestOps(SelectionKey.OP_WRITE);
                break;
            case REPLCONF_CAPA:
                System.out.println("Handshake: Received OK for REPLCONF capa.");
                queueForWrite(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
                handshakeState = HandshakeState.PSYNC;
                key.interestOps(SelectionKey.OP_WRITE);
                break;
            case PSYNC:
                if (response instanceof String && ((String) response).toUpperCase().contains("FULLRESYNC")) {
                    System.out.println("Handshake: Received FULLRESYNC.");
                } else if (response instanceof byte[]) {
                    System.out.println("Handshake: Received RDB file, now online.");
                    handshakeState = HandshakeState.ONLINE;
                }
                break;
            case ONLINE:
                if (response instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> args = (List<String>) response;

                    // A simple estimation of command size from the parsed parts.
                    long commandSize = 0;
                    for (String part : args) {
                        commandSize += part.getBytes(StandardCharsets.UTF_8).length;
                    }
                    replicationOffset += commandSize;

                    String commandName = args.get(0).toUpperCase();
                    if ("REPLCONF".equals(commandName) && args.size() > 1 && "GETACK".equalsIgnoreCase(args.get(1))) {
                        List<String> ackCommand = List.of("REPLCONF", "ACK", String.valueOf(this.replicationOffset));
                        queueForWrite(RESPUtils.buildCommand(ackCommand));
                        key.interestOps(SelectionKey.OP_WRITE);
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
}