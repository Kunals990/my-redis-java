package handler.replication;

import config.ReplicaConfig;
import config.ServerConfig;
import handler.Command;
import handler.CommandRegistry;
import handler.commands.*;
import util.RESPUtils;
import util.RESPResponseParser;

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
    private long replicationOffset = 0; // <-- The offset is now a member variable

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

            // ... (The entire handshake block in your run() method is correct) ...
            // PING
            out.write(RESPUtils.buildCommand(List.of("PING")));
            out.flush();
            parser.parse();

            // REPLCONF listening-port
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(this.myPort))));
            out.flush();
            parser.parse();

            // REPLCONF capa psync2
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
            out.flush();
            parser.parse();

            // PSYNC
            out.write(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
            out.flush();
            parser.parse(); // +FULLRESYNC
            parser.parse(); // RDB File

            startCommandReplicationLoop(out, parser);

        } catch (Throwable t) {
            System.out.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.out);
        }
    }

    private void startCommandReplicationLoop(OutputStream out, RESPParser parser) throws IOException {
        while (true) {
            // 1. Parse the next command from the master
            Object parsedCommand = parser.parse();
            if (parsedCommand == null) {
                // End of stream
                break;
            }

            // We expect an array of strings for commands
            if (!(parsedCommand instanceof List)) {
                logger.warning("Received non-array command in replication stream: " + parsedCommand);
                continue;
            }

            @SuppressWarnings("unchecked")
            List<String> args = (List<String>) parsedCommand;
            if (args.isEmpty()) {
                continue;
            }

            String cmd = args.get(0).toUpperCase();

            // 2. Handle the command
            if ("REPLCONF".equals(cmd) && "GETACK".equalsIgnoreCase(args.get(1))) {
                // Acknowledge the master's ping using our current offset
                String currentOffsetStr = Long.toString(this.replicationOffset);
                String ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                        + currentOffsetStr.length() + "\r\n" + currentOffsetStr + "\r\n";
                out.write(ack.getBytes());
                out.flush();
            } else {
                // For any other command (like SET, etc.), it's a propagated write command.
                // Execute it and then update our offset by the number of bytes it took.
                Command cmdImpl = CommandRegistry.getCommand(cmd);
                if (cmdImpl != null) {
                    cmdImpl.execute(args, null);
                } else {
                    logger.warning("Unknown replication cmd: " + cmd);
                }

                // 3. Update offset AFTER processing a write command
                this.replicationOffset += parser.getBytesReadSinceLastCommand();
            }
        }
    }

class RESPParser {
    private final InputStream in;
    private long bytesReadSinceLastCommand = 0;

    public RESPParser(InputStream in) {
        this.in = in;
    }

    public long getBytesReadSinceLastCommand() { // <-- ADD THIS GETTER
        return this.bytesReadSinceLastCommand;
    }

    public Object parse() throws IOException {
        bytesReadSinceLastCommand = 0;
        return _parse();
    }

    // ... (_parse, parseArray, parseSimpleString, etc. remain the same as the previous step)
    private Object _parse() throws IOException {
        int type = in.read();
        if (type == -1) {
            return null;
        }
        bytesReadSinceLastCommand++;

        return switch ((char) type) {
            case '+' -> parseSimpleString();
            case '*' -> parseArray();
            case '$' -> {
                int len = readInt();
                byte[] data = in.readNBytes(len);
                bytesReadSinceLastCommand += len;
                in.readNBytes(2); // CRLF
                bytesReadSinceLastCommand += 2;
                yield data;
            }
            default -> throw new IOException("Unknown RESP type: " + (char) type);
        };
    }

    private String parseSimpleString() throws IOException {
        return readLine();
    }

    private List<String> parseArray() throws IOException {
        int count = readInt();
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
                in.read(); // consume LF
                bytesReadSinceLastCommand++;
                break;
            }
            bout.write(b);
        }
        return bout.toString(StandardCharsets.US_ASCII.name());
    }

    private int readInt() throws IOException {
        String line = readLine();
        return Integer.parseInt(line);
    }
}
}