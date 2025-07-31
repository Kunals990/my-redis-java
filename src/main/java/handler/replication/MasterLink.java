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

    private void enqueue(SelectionKey key, byte[] data) {
        outbound.add(ByteBuffer.wrap(data));
        key.interestOps(SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    public void handleConnect(SelectionKey key) throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        System.out.println("Replica connected to master. Starting handshake.");

        // Pipeline the full handshake in one go:
        enqueue(key, RESPUtils.buildCommand(List.of("PING")));
        enqueue(key, RESPUtils.buildCommand(
                List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
        enqueue(key, RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
        enqueue(key, RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
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
        // Drain any queued outbound buffers
        while (!outbound.isEmpty()) {
            ByteBuffer buf = outbound.peek();
            channel.write(buf);
            if (buf.hasRemaining()) {
                // socket not ready for more, leave OP_WRITE set
                return;
            }
            outbound.poll();
        }

        // If fully handed over to ONLINE, we periodically send our ACK
        if (state == HandshakeState.ONLINE) {
            enqueue(key, RESPUtils.buildCommand(
                    List.of("REPLCONF", "ACK", Long.toString(replicationOffset))));
            return;
        }

        // No more to write right now
        key.interestOps(SelectionKey.OP_READ);
    }

    private void processResponse(SelectionKey key, Object response) throws IOException {
        // Advance the handshake state machine based on the replies
        switch (state) {
            case PING:
                state = HandshakeState.REPLCONF_PORT;
                break;
            case REPLCONF_PORT:
                state = HandshakeState.REPLCONF_CAPA;
                break;
            case REPLCONF_CAPA:
                state = HandshakeState.PSYNC_FULLRESYNC;
                break;
            case PSYNC_FULLRESYNC:
                state = HandshakeState.PSYNC_RDB;
                break;
            case PSYNC_RDB:
                state = HandshakeState.ONLINE;
                System.out.println("Replica is now ONLINE.");
                break;
            case ONLINE:
                // Once online, handle incoming GETACK or commands
                if (response instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> args = (List<String>) response;
                    String cmd = args.get(0).toUpperCase();
                    if ("REPLCONF".equals(cmd) && "GETACK".equalsIgnoreCase(args.get(1))) {
                        // enqueue an immediate ACK reply
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
