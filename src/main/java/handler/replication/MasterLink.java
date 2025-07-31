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
                // Not fully written; keep OP_WRITE set and return.
                // The selector will notify us again when the channel is ready.
                return;
            }
            // Buffer was fully written, remove it from the queue.
            outbound.poll();
        }

        // --- FIX ---
        // The queue is empty. We are done writing for now.
        // Switch back to only being interested in reading from the master.
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
                        // --- FIX START ---
                        // DO NOT write directly. Use the non-blocking queue.
                        byte[] ackCmd = RESPUtils.buildCommand(
                                List.of("REPLCONF", "ACK", Long.toString(replicationOffset)));
                        System.out.println("[slave] → QUEUEING “" + new String(ackCmd, StandardCharsets.UTF_8).trim() + "”");
                        enqueue(key, ackCmd);
                        // --- FIX END ---
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
        buf.mark();
        if (!buf.hasRemaining()) {
            buf.reset();
            return null;
        }
        if (buf.get() != '+') { // consume '+'
            buf.reset();
            return null; // Or throw an error, not a simple string
        }

        StringBuilder sb = new StringBuilder();
        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r') {
                if (buf.hasRemaining() && buf.get() == '\n') {
                    return sb.toString();
                } else {
                    // Incomplete CRLF, reset and wait for more data
                    buf.reset();
                    return null;
                }
            }
            sb.append(c);
        }

        buf.reset(); // Incomplete line
        return null;
    }

    /**
     * Read a $-style bulk string, return its contents.
     * Returns null if incomplete, after resetting the buffer position.
     */
    private String readBulkString(ByteBuffer buf) {
        buf.mark();
        if (!buf.hasRemaining() || buf.get() != '$') {
            buf.reset();
            return null;
        }

        Integer len = readIntCRLF(buf);
        if (len == null) {
            buf.reset();
            return null;
        }
        if (len == -1) { // Null bulk string
            return null;
        }

        if (buf.remaining() < len + 2) { // not all data + CRLF yet
            buf.reset();
            return null;
        }

        byte[] data = new byte[len];
        buf.get(data);
        // consume trailing CRLF
        if (buf.get() != '\r' || buf.get() != '\n') {
            // Malformed, but we can't un-read the data. For the scope of this project, we assume valid protocol.
            // In a real-world scenario, you'd handle this protocol error more gracefully.
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Read an integer until CRLF (no leading ':' or other markers).
     * Returns null if incomplete, after resetting the buffer position.
     */
    private Integer readIntCRLF(ByteBuffer buf) {
        buf.mark();
        StringBuilder sb = new StringBuilder();
        boolean isNegative = false;

        if (buf.hasRemaining()) {
            char firstChar = (char) buf.get();
            if (firstChar == '-') {
                isNegative = true;
            } else {
                sb.append(firstChar);
            }
        }

        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r') {
                if (buf.hasRemaining() && buf.get() == '\n') {
                    try {
                        int value = Integer.parseInt(sb.toString());
                        return isNegative ? -value : value;
                    } catch (NumberFormatException e) {
                        buf.reset();
                        return null;
                    }
                } else {
                    buf.reset();
                    return null;
                }
            }
            sb.append(c);
        }

        buf.reset();
        return null;
    }

    /**
     * Skip until the next CRLF (for ints or any other line).
     * Returns true if a line was skipped, false if incomplete (resets buffer).
     */
    private boolean skipLine(ByteBuffer buf) {
        buf.mark();
        while (buf.hasRemaining()) {
            if (buf.get() == '\r') {
                if (buf.hasRemaining() && buf.get() == '\n') {
                    return true;
                }
            }
        }
        // Incomplete line
        buf.reset();
        return false;
    }
}
