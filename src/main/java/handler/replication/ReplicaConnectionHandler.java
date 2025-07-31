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
            RESPParser parser = new RESPParser(in);

            // --- Robust Handshake Logic with Validation ---

            // 1. PING
            out.write(RESPUtils.buildCommand(List.of("PING")));
            out.flush();
            Object pongResponse = parser.parse();
            if (!"+PONG".equalsIgnoreCase(pongResponse.toString())) {
                throw new IOException("Handshake failed: Did not receive PONG. Got: " + pongResponse);
            }

            // 2. REPLCONF listening-port
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(this.myPort))));
            out.flush();
            Object ok1Response = parser.parse();
            if (!"+OK".equalsIgnoreCase(ok1Response.toString())) {
                throw new IOException("Handshake failed: Did not receive OK for REPLCONF port. Got: " + ok1Response);
            }

            // 3. REPLCONF capa psync2
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
            out.flush();
            Object ok2Response = parser.parse();
            if (!"+OK".equalsIgnoreCase(ok2Response.toString())) {
                throw new IOException("Handshake failed: Did not receive OK for REPLCONF capa. Got: " + ok2Response);
            }

            // 4. PSYNC
            out.write(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
            out.flush();
            Object fullResyncResponse = parser.parse();
            if (!(fullResyncResponse instanceof String) || !((String) fullResyncResponse).toUpperCase().startsWith("+FULLRESYNC")) {
                throw new IOException("Handshake failed: Did not receive FULLRESYNC. Got: " + fullResyncResponse);
            }
            parser.parse(); // Consume the RDB file bytes and ignore them.

            // --- Handshake complete, stream is now synchronized ---
            startCommandReplicationLoop(out, parser);

        } catch (Throwable t) {
            System.err.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.err);
        }
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

    // The RESPParser inner class is correct and does not need changes.
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
                    list.add(((String) item).substring(1)); // Remove the '+' prefix
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