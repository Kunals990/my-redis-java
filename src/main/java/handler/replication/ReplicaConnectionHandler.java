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

    public ReplicaConnectionHandler(String masterHost, int masterPort,int myPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.myPort = myPort;
    }


    @Override
    public void run() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = new BufferedInputStream(socket.getInputStream());

            // Create ONE parser for the entire connection
            RESPParser parser = new RESPParser(in);

            // HANDSHAKE
            // PING
            out.write(RESPUtils.buildCommand(List.of("PING")));
            out.flush();
            String pong = (String) parser.parse();
            if (!"PONG".equalsIgnoreCase(pong)) throw new IOException("Handshake failed, expected PONG");

            // REPLCONF listening-port
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "listening-port", String.valueOf(this.myPort))));
            out.flush();
            String ok1 = (String) parser.parse();
            if (!"OK".equalsIgnoreCase(ok1)) throw new IOException("Handshake failed on REPLCONF port");

            // REPLCONF capa psync2
            out.write(RESPUtils.buildCommand(List.of("REPLCONF", "capa", "psync2")));
            out.flush();
            String ok2 = (String) parser.parse();
            if (!"OK".equalsIgnoreCase(ok2)) throw new IOException("Handshake failed on REPLCONF capa");

            // PSYNC
            out.write(RESPUtils.buildCommand(List.of("PSYNC", "?", "-1")));
            out.flush();
            String fullResync = (String) parser.parse(); // Should be +FULLRESYNC...
            byte[] rdbFile = (byte[]) parser.parse();    // Should be the RDB file bytes

            // MAIN COMMAND LOOP
            startCommandReplicationLoop(out, parser); // Pass the parser, not the streams

        } catch (Throwable t) {
            System.out.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.out);
        }
    }



    private void startCommandReplicationLoop(OutputStream out, RESPParser parser) throws IOException {
        while (true) {
            // Use the single parser to read the next command
            List<String> args = (List<String>) parser.parse();
            if (args == null || args.isEmpty()) continue;

            String cmd = args.get(0).toUpperCase();

            // Note: We only increment the offset for write commands that are propagated
            // The logic to decide this should be within the command handlers themselves.
            // For now, let's assume we increment for all non-REPLCONF commands.
            if (!cmd.equals("REPLCONF")) {
                parser.recordOffset();
            }

            if ("REPLCONF".equals(cmd) && "GETACK".equalsIgnoreCase(args.get(1))) {
                // Your existing GETACK handling logic is correct
                long currentOffset = ReplicaConfig.getOffset();
                String off = Long.toString(currentOffset);
                String ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                        + off.length() + "\r\n" + off + "\r\n";
                out.write(ack.getBytes());
                out.flush();
            } else {
                Command cmdImpl = CommandRegistry.getCommand(cmd);
                if (cmdImpl != null) {
                    cmdImpl.execute(args, null);
                } else {
                    logger.warning("Unknown replication cmd: " + cmd);
                }
            }
        }
    }
}

class RESPParser {
    private final InputStream in;
    private long bytesReadSinceLastCommand = 0;

    public RESPParser(InputStream in) {
        this.in = in;
    }

    /**
     * Public-facing parse method. This is the entry point for parsing a new command.
     * It resets the byte counter and calls the internal parser.
     */
    public Object parse() throws IOException {
        bytesReadSinceLastCommand = 0; // Reset counter for a new command
        return _parse(); // Call the internal parser
    }

    /**
     * Internal parser. Does the actual work without resetting the counter.
     * Used for recursive calls.
     */
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
                yield data; // Return raw bytes for RDB file
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
            // IMPORTANT: The recursive call is to the internal _parse()
            Object item = _parse();
            if (item instanceof String) {
                list.add((String) item);
            } else if (item instanceof byte[]) {
                list.add(new String((byte[]) item, StandardCharsets.UTF_8));
            }
        }
        return list;
    }

    // Call this after processing a write command
    public void recordOffset() {
        ReplicaConfig.incrOffset(bytesReadSinceLastCommand);
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