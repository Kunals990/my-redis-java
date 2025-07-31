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

            // The main loop now uses the robust parser.
            startCommandProcessingLoop(in, out);

        } catch (Throwable t) {
            System.err.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.err);
        }
    }

    // The simple, dedicated handshake logic is correct.
    private void performHandshake(InputStream in, OutputStream out) throws IOException {
        out.write(RESPUtils.buildCommand(List.of("PING")));
        out.flush();
        readHandshakeLine(in);

        out.write(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(myPort))));
        out.flush();
        readHandshakeLine(in);

        out.write(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
        out.flush();
        readHandshakeLine(in);

        out.write(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
        out.flush();
        readHandshakeLine(in); // Consume +FULLRESYNC
        readRDBFile(in);     // Consume RDB
    }

    private String readHandshakeLine(InputStream in) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            bout.write(b);
            if (bout.size() >= 2 && bout.toByteArray()[bout.size() - 2] == '\r' && bout.toByteArray()[bout.size() - 1] == '\n') {
                break;
            }
        }
        return bout.toString(StandardCharsets.UTF_8).trim();
    }

    private void readRDBFile(InputStream in) throws IOException {
        int type = in.read();
        if (type != '$') throw new IOException("Expected '$' for RDB file");
        String lengthStr = readHandshakeLine(in);
        int length = Integer.parseInt(lengthStr);
        in.readNBytes(length);
    }

    // This is the corrected main processing loop.
    private void startCommandProcessingLoop(InputStream in, OutputStream out) throws IOException {
        RESPParser parser = new RESPParser(in);
        while (true) {
            Object parsedCommand = parser.parse();
            if (parsedCommand == null) break;

            long bytesParsed = parser.getBytesReadSinceLastCommand();
            if (!(parsedCommand instanceof List)) continue;

            @SuppressWarnings("unchecked")
            List<String> args = (List<String>) parsedCommand;
            if (args.isEmpty()) continue;
            System.out.println("in 1");
            String cmd = args.get(0).toUpperCase();
            System.out.println("cmd is "+cmd);

            if ("REPLCONF".equals(cmd) && args.size() > 1 && "GETACK".equalsIgnoreCase(args.get(1))) {
                System.out.println("in 2");
                // Respond with the offset *before* this GETACK command was processed.
                String offsetBeforeThisCommand = Long.toString(this.replicationOffset);
                List<String> ackCommand = List.of("REPLCONF", "ACK", offsetBeforeThisCommand);
                out.write(RESPUtils.buildCommand(ackCommand));
                out.flush();
            } else {
                // For propagated write commands, just execute them.
                System.out.println("above command");
                Command cmdImpl = CommandRegistry.getCommand(cmd);
                System.out.println("Command is "+cmdImpl);
                if (cmdImpl != null) {
                    cmdImpl.execute(args, null);
                }
            }

            // *** THE CRITICAL FIX ***
            // ALWAYS increment the offset by the bytes of the command we just processed.
            this.replicationOffset += bytesParsed;
        }
    }

    // The robust RESPParser for the main loop is correct.
    class RESPParser {
        // ... This class does not need any changes from the previous version ...
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