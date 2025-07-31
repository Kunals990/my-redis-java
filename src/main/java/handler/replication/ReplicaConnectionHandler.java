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

            performHandshake(in, out);

            RESPParser parser = new RESPParser(in);
            startCommandReplicationLoop(out, parser);

        } catch (Throwable t) {
            System.err.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.err);
        }
    }

    private void performHandshake(InputStream in, OutputStream out) throws IOException {
        out.write(RESPUtils.buildCommand(List.of("PING")));
        out.flush();
        readHandshakeResponse(in, "+PONG");

        out.write(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
        out.flush();
        readHandshakeResponse(in, "+OK");

        out.write(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
        out.flush();
        readHandshakeResponse(in, "+OK");

        out.write(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
        out.flush();
        readHandshakeResponse(in, "+FULLRESYNC");
        readRDBFile(in);
    }

    // A simple, dedicated reader for the predictable handshake responses.
    // *** FIX IS HERE: Changed from 'void' to 'String' and added 'return response;' ***
    private String readHandshakeResponse(InputStream in, String expectedPrefix) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            bout.write(b);
            if (bout.size() >= 2 && bout.toByteArray()[bout.size() - 2] == '\r' && bout.toByteArray()[bout.size() - 1] == '\n') {
                break;
            }
        }
        String response = bout.toString(StandardCharsets.UTF_8).trim();
        if (!expectedPrefix.isEmpty() && !response.toUpperCase().startsWith(expectedPrefix)) {
            throw new IOException("Handshake failed. Expected " + expectedPrefix + " but got " + response);
        }
        return response; // <-- ADDED THIS RETURN
    }

    private void readRDBFile(InputStream in) throws IOException {
        int type = in.read();
        if (type != '$') throw new IOException("Expected '$' for RDB file");

        // This line will now compile correctly
        String lengthStr = readHandshakeResponse(in, "");
        int length = Integer.parseInt(lengthStr);
        in.readNBytes(length);
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
                List<String> ackCommand = List.of("REPLCONF", "ACK", currentOffsetStr);
                out.write(RESPUtils.buildCommand(ackCommand));
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

    // The RESPParser is correct and does not need changes.
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
                case '+' -> readLine();
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
                    list.add((String) item);
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