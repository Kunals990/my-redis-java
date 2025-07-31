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
        System.out.println("REPLICA: Connection handler thread started."); // <-- ADD THIS
        try (Socket socket = new Socket(masterHost, masterPort)) {
            System.out.println("REPLICA: Socket connected to master at " + masterHost + ":" + masterPort); // <-- ADD THIS

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            completeHandShake1(out, in);
            System.out.println("REPLICA: Handshake 1 (PING) complete."); // <-- ADD THIS

            completeHandShake2(out, in);
            System.out.println("REPLICA: Handshake 2 (REPLCONF port) complete."); // <-- ADD THIS

            completeHandShake3(out, in);
            System.out.println("REPLICA: Handshake 3 (REPLCONF capa) complete."); // <-- ADD THIS

            completeHandShake4(out, in);
            System.out.println("REPLICA: Handshake 4 (PSYNC) complete."); // <-- ADD THIS

            startCommandReplicationLoop(out, in);

        } catch (Throwable t) { // <-- CATCH THROWABLE, NOT JUST IOEXCEPTION
            // This will catch any and all errors and print a full stack trace.
            System.out.println("REPLICA: CRITICAL ERROR IN REPLICA THREAD");
            t.printStackTrace(System.out);
        }
    }

    private void completeHandShake1(OutputStream out, InputStream in) throws IOException {
        List<String> req = List.of("PING");
        out.write(RESPUtils.buildCommand(req));
        out.flush();

        byte[] buf = new byte[1024];
        int n = in.read(buf);
        String resp = RESPResponseParser.parseSimpleString(buf, n);
        if (!"PONG".equalsIgnoreCase(resp)) {
            throw new IOException("Handshake1 failed, expected PONG, got: " + resp);
        }
    }

    private void completeHandShake2(OutputStream out, InputStream in) throws IOException {
        List<String> req = List.of("REPLCONF", "listening-port", String.valueOf(this.myPort));
        out.write(RESPUtils.buildCommand(req));
        out.flush();

        byte[] buf = new byte[1024];
        int n = in.read(buf);
        String resp = RESPResponseParser.parseSimpleString(buf, n);
        if (!"OK".equalsIgnoreCase(resp)) {
            throw new IOException("Handshake2 failed, expected OK, got: " + resp);
        }
    }

    private void completeHandShake3(OutputStream out, InputStream in) throws IOException {
        List<String> req = List.of("REPLCONF", "capa", "psync2");
        out.write(RESPUtils.buildCommand(req));
        out.flush();

        byte[] buf = new byte[1024];
        int n = in.read(buf);
        String resp = RESPResponseParser.parseSimpleString(buf, n);
        if (!"OK".equalsIgnoreCase(resp)) {
            throw new IOException("Handshake3 failed, expected OK, got: " + resp);
        }
    }

    private void completeHandShake4(OutputStream out, InputStream in) throws IOException {
        // Send PSYNC ? -1
        List<String> req = List.of("PSYNC", "?", "-1");
        out.write(RESPUtils.buildCommand(req));
        out.flush();

        // Log what is read for the FULLRESYNC line
        String fullResyncResponse = readLine(in);
        System.out.println("REPLICA READ LINE: " + fullResyncResponse);
        if (!fullResyncResponse.startsWith("+FULLRESYNC")) {
            throw new IOException("Handshake4 failed, expected +FULLRESYNC, got: " + fullResyncResponse);
        }

        // Log the RDB header read
        int dollar = in.read();
        System.out.println("REPLICA READ CHAR: " + (char)dollar);
        if (dollar != '$') {
            throw new IOException("Expected '$' for RDB bulk, got: " + (char)dollar);
        }
        String lenLine = readLine(in);
        System.out.println("REPLICA READ LINE: " + lenLine);
        int len = Integer.parseInt(lenLine);
        System.out.println("REPLICA PARSED RDB LENGTH: " + len);

        // This is the corrected, robust way to read exactly 'len' bytes
        byte[] rdbBuffer = new byte[len];
        int totalBytesRead = 0;
        while (totalBytesRead < len) {
            int bytesRead = in.read(rdbBuffer, totalBytesRead, len - totalBytesRead);
            if (bytesRead == -1) {
                throw new IOException("Unexpected end of stream while reading RDB payload.");
            }
            totalBytesRead += bytesRead;
        }
        System.out.println("REPLICA ATTEMPTED TO READ " + len + " RDB BYTES, ACTUALLY READ: " + totalBytesRead);

        // The handshake is now complete. The stream is correctly positioned.
    }

    private String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int nl = in.read();
                if (nl != '\n') throw new IOException("Expected LF after CR");
                break;
            }
            bout.write(b);
        }
        return bout.toString(StandardCharsets.US_ASCII.name());
    }

    private void startCommandReplicationLoop(OutputStream out, InputStream in) throws IOException {
        RESPParser parser = new RESPParser(in);
        while (true) {
            long offset = ReplicaConfig.getOffset();
            List<String> args = parser.parseArray();
            if (args == null || args.isEmpty()) continue;

            String cmd = args.get(0).toUpperCase();

            if ("REPLCONF".equals(cmd)
                    && args.size() == 3
                    && "GETACK".equalsIgnoreCase(args.get(1))
                    && "*".equals(args.get(2))) {

                String off = Long.toString(offset);
                String ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                        + off.length() + "\r\n" + off + "\r\n";
                out.write(ack.getBytes());
                out.flush();
                continue;
            }

            Command cmdImpl = CommandRegistry.getCommand(cmd);
            if (cmdImpl != null) {
                cmdImpl.execute(args, null);
            } else {
                logger.warning("Unknown replication cmd: " + cmd);
            }
        }
    }
}

class RESPParser {
    private final InputStream in;
    private long bytesRead = 0;

    public RESPParser(InputStream in) {
        this.in = in;
    }

    public List<String> parseArray() throws IOException {
        bytesRead = 0;

        int b = in.read(); bytesRead++;
        if (b == -1 || b != '*') return null;

        int count = readInt();

        List<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int type = in.read(); bytesRead++;
            if (type != '$') throw new IOException("Expected '$' but got " + (char)type);
            int len = readInt();
            byte[] data = in.readNBytes(len); bytesRead += len;
            in.readNBytes(2); bytesRead += 2;
            list.add(new String(data, StandardCharsets.UTF_8));
        }
        ReplicaConfig.incrOffset(bytesRead);
        return list;
    }


    private int readInt() throws IOException {
        int result = 0;
        int b;
        while ((b = in.read()) != -1) {
            bytesRead++;
            if (b == '\r') {
                in.read(); bytesRead++;
                break;
            }
            if (b < '0' || b > '9') throw new IOException("Invalid digit in length: " + b);
            result = result * 10 + (b - '0');
        }
        if (b == -1) throw new IOException("Unexpected EOF reading length");
        return result;
    }
}
