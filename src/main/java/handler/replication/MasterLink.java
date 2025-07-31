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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class MasterLink {
    private final SocketChannel channel;
    private final Selector selector;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private long replicationOffset = 0;
    private final int myPort;

    private final Deque<ByteBuffer> outbound = new ArrayDeque<>();

    private enum HandshakeState { PING, REPLCONF_PORT, REPLCONF_CAPA, PSYNC_FULLRESYNC, PSYNC_RDB, ONLINE }
    private HandshakeState state = HandshakeState.PING;

    public MasterLink(String host, int port, int myPort, Selector selector) throws IOException {
        this.selector = selector;
        this.channel = SocketChannel.open();
        this.channel.configureBlocking(false);
        this.channel.register(selector, SelectionKey.OP_CONNECT, this);
        this.channel.connect(new InetSocketAddress(host, port));
        this.myPort = myPort;
    }

    private void enqueue(SelectionKey key, byte[] data) {
        outbound.add(ByteBuffer.wrap(data));
        key.interestOps(SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    public void handleConnect(SelectionKey key) throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
//        System.out.println("Replica connected to master. Starting handshake.");
//        send(key, RESPUtils.buildCommand(List.of("PING")));
        System.out.println("Replica connected to master. Starting handshake.");
        enqueue(key, RESPUtils.buildCommand(List.of("PING")));
    }

    public void handleRead(SelectionKey key) throws IOException {
        int bytesRead = channel.read(readBuffer);
        if (bytesRead == -1) {
            key.cancel();
            channel.close();
            return;
        }
        readBuffer.flip();
        RESPParser parser = new RESPParser(readBuffer);

        while (readBuffer.hasRemaining()) {
            Object parsed = parser.parse();
            if (parsed == null) break;
            processResponse(key, parsed);
        }
        readBuffer.compact();
    }

    public void handleWrite(SelectionKey key) throws IOException {
        // This is now only for ACKs
//        String ack = Long.toString(this.replicationOffset);
//        byte[] ackCommand = RESPUtils.buildCommand(List.of("REPLCONF", "ACK", ack));
//        channel.write(ByteBuffer.wrap(ackCommand));
//        key.interestOps(SelectionKey.OP_READ);

        while (!outbound.isEmpty()) {
            ByteBuffer buf = outbound.peek();
            channel.write(buf);
            if (buf.hasRemaining()) {
                return;}
            outbound.poll();
        }

        if (state == HandshakeState.ONLINE) {
//                String ack = Long.toString(this.replicationOffset);
//                enqueue(key, RESPUtils.buildCommand(List.of("REPLCONF", "ACK", ack)));
                enqueue(key,RESPUtils.buildCommand(List.of("REPLCONF", "ACK", Long.toString(replicationOffset))));
                return; // OP_WRITE remains set so that next select() will come back here
        }

        key.interestOps(SelectionKey.OP_READ);
    }

//    private void send(SelectionKey key, byte[] data) throws IOException {
//        channel.write(ByteBuffer.wrap(data));
//        key.interestOps(SelectionKey.OP_READ);
//    }

    private void processResponse(SelectionKey key, Object response) throws IOException {
        // Here we manage the handshake as a state machine
        switch (state) {
            case PING:
                state = HandshakeState.REPLCONF_PORT;
//                state = HandshakeState.REPLCONF_PORT;
//                send(key, RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", "6380")));
                enqueue(key, RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
                break;
            case REPLCONF_PORT:
//                state = HandshakeState.REPLCONF_CAPA;
//                send(key, RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
                state = HandshakeState.REPLCONF_CAPA;
                enqueue(key, RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
                break;
            case REPLCONF_CAPA:
//                state = HandshakeState.PSYNC_FULLRESYNC;
//                send(key, RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
                state = HandshakeState.PSYNC_FULLRESYNC;
                enqueue(key, RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
                break;
            case PSYNC_FULLRESYNC:
                state = HandshakeState.PSYNC_RDB;
                // We've received FULLRESYNC, now we just wait for the RDB file.
                break;
            case PSYNC_RDB:
                state = HandshakeState.ONLINE;
                System.out.println("Replica is now ONLINE.");
                break;
            case ONLINE:
                if (response instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> args = (List<String>) response;
                    String commandName = args.get(0).toUpperCase();

                    if ("REPLCONF".equals(commandName) && "GETACK".equalsIgnoreCase(args.get(1))) {
                        key.interestOps(SelectionKey.OP_WRITE); // Ask to be notified when we can write the ACK
                    } else {
                        Command command = CommandRegistry.getCommand(commandName);
                        if (command != null) {
                            command.execute(args, null);
                        }
                    }
                }
                break;
        }
    }
}