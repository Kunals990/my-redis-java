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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class MasterLink {
    private final SocketChannel channel;
    private final Selector selector;
    private final int myPort;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private long replicationOffset = 0;
    private final Deque<ByteBuffer> outbound = new ArrayDeque<>();

    private enum HandshakeState {
        PING, REPLCONF_PORT, REPLCONF_CAPA, PSYNC_FULLRESYNC, PSYNC_RDB, ONLINE
    }
    private HandshakeState state = HandshakeState.PING;

    public MasterLink(String host, int port, int myPort, Selector selector) throws IOException {
        this.selector = selector;
        this.myPort   = myPort;
        this.channel  = SocketChannel.open();
        this.channel.configureBlocking(false);
        this.channel.register(selector, SelectionKey.OP_CONNECT, this);
        this.channel.connect(new InetSocketAddress(host, port));
    }

    /** Queue up a frame and wake the selector so handleWrite will fire. */
    private void enqueue(SelectionKey key, byte[] data) {
        outbound.add(ByteBuffer.wrap(data));
        key.interestOps(SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    public void handleConnect(SelectionKey key) throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        // 1) On connect, send only PING:
        enqueue(key, RESPUtils.buildCommand(List.of("PING")));
    }

    public void handleRead(SelectionKey key) throws IOException {
        int n = channel.read(readBuffer);
        if (n == -1) {
            key.cancel();
            channel.close();
            return;
        }
        readBuffer.flip();
        RESPParser parser = new RESPParser(readBuffer);

        // Process all complete frames
        while (readBuffer.hasRemaining()) {
            Object resp = parser.parse();
            if (resp == null) break;
            processResponse(key, resp);
        }
        readBuffer.compact();
    }

    public void handleWrite(SelectionKey key) throws IOException {
        // Drain queued buffers
        while (!outbound.isEmpty()) {
            ByteBuffer b = outbound.peek();
            channel.write(b);
            if (b.hasRemaining()) {
                // Not fully written; keep OP_WRITE set
                return;
            }
            outbound.poll();
        }

        // If we're ONLINE, periodically ACK
        if (state == HandshakeState.ONLINE) {
            enqueue(key, RESPUtils.buildCommand(
                    List.of("REPLCONF", "ACK", Long.toString(replicationOffset))));
            return;
        }

        // Otherwise, back to reading and wait for next handshake reply
        key.interestOps(SelectionKey.OP_READ);
    }

    private void processResponse(SelectionKey key, Object resp) throws IOException {
        // Handshake state transitions, only enqueue the *one* next frame.
        switch (state) {
            case PING:
                // Expecting +PONG
                if (resp instanceof String && "PONG".equals(resp)) {
                    state = HandshakeState.REPLCONF_PORT;
                    enqueue(key, RESPUtils.buildCommand(
                            List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
                }
                break;

            case REPLCONF_PORT:
                // Expecting +OK to port
                if (resp instanceof String && "OK".equals(resp)) {
                    state = HandshakeState.REPLCONF_CAPA;
                    enqueue(key, RESPUtils.buildCommand(
                            List.of("REPLCONF", "capa", "psync2")));
                }
                break;

            case REPLCONF_CAPA:
                // Expecting +OK to capa
                if (resp instanceof String && "OK".equals(resp)) {
                    state = HandshakeState.PSYNC_FULLRESYNC;
                    enqueue(key, RESPUtils.buildCommand(
                            List.of("PSYNC", "?", "-1")));
                }
                break;

            case PSYNC_FULLRESYNC:
                // Expecting +FULLRESYNC <runid> <offset>
                if (resp instanceof String && ((String)resp).startsWith("FULLRESYNC")) {
                    state = HandshakeState.PSYNC_RDB;
                    // The tester will now send the bulk-RDB; we just wait for it
                }
                break;

            case PSYNC_RDB:
                // After reading the $<len>\r\n<data>\r\n, parser returns the data as a byte[] or String.
                // At that point we transition ONLINE.
                state = HandshakeState.ONLINE;
                System.out.println("Replica is now ONLINE.");
                break;

            case ONLINE:
                // Handle GETACK or commands
                if (resp instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> args = (List<String>) resp;
                    String cmd = args.get(0).toUpperCase();
                    if ("REPLCONF".equals(cmd) && "GETACK".equalsIgnoreCase(args.get(1))) {
                        enqueue(key, RESPUtils.buildCommand(
                                List.of("REPLCONF", "ACK", Long.toString(replicationOffset))));
                    } else {
                        Command c = CommandRegistry.getCommand(cmd);
                        if (c != null) {
                            c.execute(args, null);
                        }
                    }
                }
                break;
        }
    }
}
