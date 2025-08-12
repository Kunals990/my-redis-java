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
    private String replicationId = "?";

    private enum HandshakeState {
        PING, REPLCONF_PORT, REPLCONF_CAPA, PSYNC, RECEIVING_RDB, ONLINE
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
        if (channel.isConnectionPending()) channel.finishConnect();
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
                String s = readSimpleString(readBuffer);
                if (s == null) break;
                processResponse(key, s);
            } else if (lead == ':') {
                if (!skipLine(readBuffer)) break;
            } else if (lead == '*') {
                Object arrObj = new RESPParser(readBuffer).parse();
                if (arrObj == null) break;
                if (arrObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> arr = (List<String>) arrObj;
                    processResponse(key, arr);
                } else break;
            } else if (lead == '$') {
                String bulk = readBulkString(readBuffer);
                if (bulk == null) break;
                processResponse(key, bulk);
            } else break;
        }

        readBuffer.compact();
    }

    public void handleWrite(SelectionKey key) throws IOException {
        while (!outbound.isEmpty()) {
            ByteBuffer b = outbound.peek();
            channel.write(b);
            if (b.hasRemaining()) return;
            outbound.poll();
        }
        key.interestOps(SelectionKey.OP_READ);
    }

    private void processResponse(SelectionKey key, Object resp) throws IOException {
        switch (state) {
            case PING:
                if (resp instanceof String && "PONG".equals(resp)) {
                    state = HandshakeState.REPLCONF_PORT;
                    enqueue(key, RESPUtils.buildCommand(
                            List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
                }
                break;

            case REPLCONF_PORT:
                if (resp instanceof String && "OK".equals(resp)) {
                    state = HandshakeState.REPLCONF_CAPA;
                    enqueue(key, RESPUtils.buildCommand(
                            List.of("REPLCONF", "capa", "psync2")));
                }
                break;

            case REPLCONF_CAPA:
                if (resp instanceof String && "OK".equals(resp)) {
                    state = HandshakeState.PSYNC;
                    enqueue(key, RESPUtils.buildCommand(
                            List.of("PSYNC", "?", "-1")));
                }
                break;

            case PSYNC:
                if (resp instanceof String && ((String) resp).startsWith("+FULLRESYNC")) {
                    String[] parts = ((String) resp).split(" ");
                    replicationId = parts[1];
                    replicationOffset = Long.parseLong(parts[2]);
                    state = HandshakeState.RECEIVING_RDB;
                }
                break;

            case RECEIVING_RDB:
                // When we receive the RDB file (as byte array)
                if (resp instanceof byte[]) {
                    // Process RDB file here if needed
                    state = HandshakeState.ONLINE;
                    System.out.println("Replica is now ONLINE.");

                    // Send initial ACK immediately after processing RDB
                    byte[] ackCommand = RESPUtils.buildCommand(
                            List.of("REPLCONF", "ACK", String.valueOf(replicationOffset)));
                    enqueue(key, ackCommand);
                }
                break;

            case ONLINE:
                if (resp instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> args = (List<String>) resp;
                    String cmd = args.get(0).toUpperCase();

                    // Calculate bytes received for this command
                    int cmdSize = calculateRespCommandSize(args);
                    replicationOffset += cmdSize;

                    if ("REPLCONF".equals(cmd) && args.size() > 1 && "GETACK".equals(args.get(1))) {
                        // Respond to GETACK with current offset
                        byte[] ackCommand = RESPUtils.buildCommand(
                                List.of("REPLCONF", "ACK", String.valueOf(replicationOffset)));
                        enqueue(key, ackCommand);
                    } else {
                        // Process normal command
                        Command c = CommandRegistry.getCommand(cmd);
                        if (c != null) c.execute(args, null);
                    }
                } else if (resp instanceof String) {
                    // Handle simple string commands (like PING)
                    String cmd = (String) resp;
                    // Add length of command in RESP format: +PING\r\n
                    replicationOffset += 2 + cmd.length() + 2;

                    if ("PING".equals(cmd)) {
                        Command c = CommandRegistry.getCommand("PING");
                        if (c != null) c.execute(List.of("PING"), null);
                    }
                }
                break;
        }
    }

    // Calculate the size of a RESP command
    private int calculateRespCommandSize(List<String> args) {
        int size = 1 + String.valueOf(args.size()).length() + 2; // *<count>\r\n

        for (String arg : args) {
            size += 1 + String.valueOf(arg.length()).length() + 2; // $<len>\r\n
            size += arg.length() + 2; // <data>\r\n
        }

        return size;
    }

    private String readSimpleString(ByteBuffer buf) {
        buf.mark();
        if (!buf.hasRemaining()) { buf.reset(); return null; }
        if (buf.get() != '+') { buf.reset(); return null; }
        StringBuilder sb = new StringBuilder();
        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r') {
                if (buf.hasRemaining() && buf.get() == '\n') return sb.toString();
                buf.reset(); return null;
            }
            sb.append(c);
        }
        buf.reset(); return null;
    }

    private Object readBulkString(ByteBuffer buf) {
        buf.mark();
        if (!buf.hasRemaining() || buf.get() != '$') { buf.reset(); return null; }
        Integer len = readIntCRLF(buf);
        if (len == null) { buf.reset(); return null; }
        if (len == -1) return null;

        // Check if there are enough bytes for the entire bulk string
        if (buf.remaining() < len + 2) { buf.reset(); return null; }

        // RDB file handling
        if (state == HandshakeState.RECEIVING_RDB) {
            byte[] data = new byte[len];
            buf.get(data);
            buf.get(); buf.get(); // Skip CRLF

            // Process RDB file
            state = HandshakeState.ONLINE;
            System.out.println("Replica is now ONLINE.");

            // Send initial ACK immediately after processing RDB
            try {
                byte[] ackCommand = RESPUtils.buildCommand(
                        List.of("REPLCONF", "ACK", String.valueOf(replicationOffset)));
                outbound.add(ByteBuffer.wrap(ackCommand));
                SelectionKey key = channel.keyFor(selector);
                key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                selector.wakeup();
            } catch (Exception e) {
                System.err.println("Error sending initial ACK: " + e.getMessage());
            }

            return data;
        } else {
            // Normal bulk string handling
            byte[] data = new byte[len];
            buf.get(data);
            buf.get(); buf.get(); // Skip CRLF
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private Integer readIntCRLF(ByteBuffer buf) {
        buf.mark();
        StringBuilder sb = new StringBuilder();
        while (buf.hasRemaining()) {
            char c = (char) buf.get();
            if (c == '\r') {
                if (!buf.hasRemaining()) { buf.reset(); return null; }
                buf.get();
                try { return Integer.parseInt(sb.toString()); } catch (NumberFormatException e) { return null; }
            }
            sb.append(c);
        }
        buf.reset(); return null;
    }

    private boolean skipLine(ByteBuffer buf) {
        buf.mark();
        while (buf.hasRemaining()) {
            if (buf.get() == '\r') {
                if (buf.hasRemaining() && buf.get() == '\n') return true;
            }
        }
        buf.reset(); return false;
    }
}
