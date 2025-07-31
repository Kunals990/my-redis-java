package handler.replication;

import handler.Command;
import handler.CommandRegistry;
import util.RESPUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ReplicaConnectionHandler implements Runnable {
    private static final Logger logger = Logger.getLogger(ReplicaConnectionHandler.class.getName());
    private final String masterHost;
    private final int masterPort;
    private final int myPort;
    private long replicationOffset = 0;

    public ReplicaConnectionHandler(String masterHost, int masterPort, int myPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.myPort = myPort;
    }

    @Override
    public void run() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = new BufferedInputStream(socket.getInputStream());

            // --- Phase 1: Simple Handshake using a dedicated reader ---
            // 1. PING
            out.write(RESPUtils.buildCommand(List.of("PING")));
            out.flush();
            readHandshakeResponse(in, "+PONG");

            // 2. REPLCONF listening-port
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(this.myPort))));
            out.flush();
            readHandshakeResponse(in, "+OK");

            // 3. REPLCONF capa psync2
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
            out.flush();
            readHandshakeResponse(in, "+OK");

            // 4. PSYNC
            out.write(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
            out.flush();
            readHandshakeResponse(in, "+FULLRESYNC");

            // 5. Read and discard the RDB file
            readRDBFile(in);

            // --- Phase 2: Handshake complete, switch to robust parser for main loop ---
            RESPParser parser = new RESPParser(in);
            startCommandReplicationLoop(out, parser);

        } catch (Throwable t) {
            System.err.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.err);
        }
    }

    // A simple, dedicated reader for the predictable handshake responses.
    private void readHandshakeResponse(InputStream in, String expectedPrefix) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            bout.write(b);
            if (bout.size() >= 2 && bout.toByteArray()[bout.size() - 2] == '\r' && bout.toByteArray()[bout.size() - 1] == '\n') {
                break;
            }
        }
        String response = bout.toString(StandardCharsets.UTF_8).trim();
        if (!response.toUpperCase().startsWith(expectedPrefix)) {
            throw new IOException("Handshake failed. Expected " + expectedPrefix + " but got " + response);
        }
    }

    // A dedicated method to read and discard the RDB file payload.
    private void readRDBFile(InputStream in) throws IOException {
        int type = in.read();
        if (type != '$') {
            throw new IOException("Expected '$' for RDB file, got: " + (char)type);
        }
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read(); // consume LF
                break;
            }
            bout.write(b);
        }
        int length = Integer.parseInt(bout.toString());
        in.readNBytes(length); // Read and discard the RDB payload
    }

    private void startCommandReplicationLoop(OutputStream out, RESPParser parser) throws IOException {
        while (true) {
            Object parsedCommand = parser.parse();
            if (parsedCommand == null) break;

            long bytesParsed = parser.getBytesReadSinceLastCommand();
            if (!(parsedCommand instanceof List)) continue;

            @SuppressWarnings("unchecked")
            List<String> args = (List<String>) parsedCommand;
            if (args.isEmpty()) continue;

            String cmd = args.get(0).toUpperCase();

            if ("REPLCONF".equals(cmd) && args.size() > 1 && "GETACK".equalsIgnoreCase(args.get(1))) {
                String currentOffsetStr = Long.toString(this.replicationOffset);
                String ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                        + currentOffsetStr.length() + "\r\n" + currentOffsetStr + "\r\n";
                out.write(ack.getBytes());
                out.flush();
            } else {
                Command cmdImpl = CommandRegistry.getCommand(cmd);
                if (cmdImpl != null) {
                    cmdImpl.execute(args, null);
                }
                this.replicationOffset += bytesParsed;
            }
        }
    }

    // The robust RESPParser is now only used for the main command loop.
    // It is correct and does not need changes.
    class RESPParser {
        private final InputStream in;
        private long bytesReadSinceLastCommand = 0;

        public RESPParser(InputStream in) { this.in = in; }
        public long getBytesReadSinceLastCommand() { return this.bytesReadSinceLastCommand; }

        public Object parse() throws IOException {
            bytesReadSinceLastCommand = 0;
            return _parse();
        }

        private Object _parse() throws IOException {
            int type = in.read();
            if (type == -1) return null;
            bytesReadSinceLastCommand++;

            return switch ((char) type) {
                case '+' -> "+" + readLine();
                case '*' -> parseArray();
                case '$' -> {
                    int len = readInt();
                    if (len == -1) yield null;
                    byte[] data = in.readNBytes(len);
                    bytesReadSinceLastCommand += len;
                    in.readNBytes(2); // CRLF
                    bytesReadSinceLastCommand += 2;
                    yield data;
                }
                default -> throw new IOException("Unknown RESP type: " + (char) type);
            };
        }

        private List<String> parseArray() throws IOException {
            int count = readInt();
            if (count == -1) return null;
            List<String> list = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Object item = _parse();
                if (item instanceof String) {
                    list.add(((String) item).substring(1));
                } else if (item instanceof byte[]) {
                    list.add(new String((byte[]) item, StandardCharsets.UTF_8));
                }
            }
            return list;
        }

        private String readLine() throws IOException {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            int b;
            while ((b = in.read()) != -1) {
                bytesReadSinceLastCommand++;
                if (b == '\r') {
                    in.read();
                    bytesReadSinceLastCommand++;
                    break;
                }
                bout.write(b);
            }
            return bout.toString(StandardCharsets.UTF_8);
        }

        private int readInt() throws IOException {
            String line = readLine();
            if (line == null || line.isEmpty()) return -1;
            return Integer.parseInt(line);
        }
    }
}