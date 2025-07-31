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
        key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
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
        int bytesRead = channel.read(readBuffer);
        if (bytesRead == -1) {
            key.cancel();
            channel.close();
            return;
        }
        readBuffer.flip();

        while (readBuffer.hasRemaining()) {
            readBuffer.mark();
            byte lead = readBuffer.get();
            readBuffer.reset();

            if (lead == '+') {
                // simple string: +LINE\r\n
                String s = readSimpleString(readBuffer);
                if (s == null) break;
                processResponse(key, s);

            } else if (lead == ':') {
                // integer: :123\r\n — we don't really need these, skip them
                if (!skipLine(readBuffer)) break;

            } else if (lead == '*') {
                // array
                Object arrObj = new RESPParser(readBuffer).parse();
                if (arrObj == null) break;
                if (arrObj instanceof List) {
                    List<String> arr = (List<String>) arrObj;
                    processResponse(key, arr);
                } else {
                    break;
                }

            } else if (lead == '$') {
                // bulk string: $LEN\r\n<data>\r\n
                String bulk = readBulkString(readBuffer);
                if (bulk == null) break;
                processResponse(key, bulk);

            } else {
                // unknown / incomplete
                break;
            }
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
                    if ("REPLCONF".equalsIgnoreCase(cmd) && "GETACK".equalsIgnoreCase(args.get(1))) {
                        enqueue(key, RESPUtils.buildCommand(
                                List.of("REPLCONF", "ACK", Long.toString(replicationOffset))));
                        handleWrite(key);
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

    /** Read until CRLF and return the interior (no '+' or CRLF), or null if incomplete */
    private String readSimpleString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder();
        if (!buf.hasRemaining()) return null;
        buf.get(); // consume '+'
        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r' && buf.hasRemaining() && (char) buf.get() == '\n') {
                return sb.toString();
            }
            sb.append(c);
        }
        return null; // incomplete
    }

    /** Read a $-style bulk string, return its contents or null if incomplete */
    private String readBulkString(ByteBuffer buf) {
        // Read length
        if (!buf.hasRemaining() || buf.get() != '$') return null;
        Integer len = readIntCRLF(buf);
        if (len == null) return null;
        if (buf.remaining() < len + 2) return null; // not all data yet
        byte[] data = new byte[len];
        buf.get(data);
        // consume trailing CRLF
        buf.get(); buf.get();
        return new String(data, StandardCharsets.UTF_8);
    }

    /** Read an integer until CRLF (no leading ':'), or null if incomplete */
    private Integer readIntCRLF(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder();
        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r' && buf.hasRemaining() && (char) buf.get() == '\n') {
                try { return Integer.parseInt(sb.toString()); }
                catch (NumberFormatException e) { return null; }
            }
            sb.append(c);
        }
        return null;
    }

    /** Skip until the next CRLF (for ints or any other line), return true if done, false if incomplete */
    private boolean skipLine(ByteBuffer buf) {
        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r' && buf.hasRemaining() && (char) buf.get() == '\n') {
                return true;
            }
        }
        return false;
    }
}
